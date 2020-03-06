# frozen_string_literal: true

require 'puma/runner'
require 'puma/util'
require 'puma/plugin'
require 'puma/worker_generator'

require 'time'

module Puma
  # This class is instantiated by the `Puma::Launcher` and used
  # to boot and serve a Ruby application when puma "workers" are needed
  # i.e. when using multi-processes. For example `$ puma -w 5`
  #
  # At the core of this class is running an instance of `Puma::Server` which
  # gets created via the `start_server` method from the `Puma::Runner` class
  # that this inherits from.
  #
  # An instance of this class will spawn the number of processes passed in
  # via the `spawn_workers` method call. Each worker will have it's own
  # instance of a `Puma::Server`.
  class Cluster < Runner
    def initialize(cli, events)
      super cli, events

      @phase = 0

      @phased_state = :idle
      @phased_restart = false
    end

    def stop_worker_generators
      # TODO: kill with SIGKILL eventually
      if @current_worker_generator_pid
        Process.kill 'TERM', @current_worker_generator_pid
      end
      if @next_worker_generator_pid
        Process.kill 'TERM', @next_worker_generator_pid
      end

      begin
        loop do
          wait_worker_generators
          break if @current_worker_generator_pid.nil? && @next_worker_generator_pid.nil?
          sleep 0.2
        end
      rescue Interrupt
        log "! Cancelled waiting for worker generators"
      end
    end

    def start_phased_restart
      @phase += 1
      log "- Starting phased worker restart, phase: #{@phase}"
    end

    def redirect_io
      super

      # TODO send HUP to all stem cells
    end

    def spawn_initial_worker_generator
      @current_worker_generator = WorkerGenerator.new phase: @phase,
                                                      num_desired_workers: @options[:workers],
                                                      options: @options,
                                                      launcher: @launcher,
                                                      events: @events
      pid = fork { @current_worker_generator.run }

      if !pid
        log "! Complete inability to spawn new worker generator detected"
        log "! Seppuku is the only choice."
        exit! 1
      end

      debug "Spawned worker generator: #{pid}"

      @current_worker_generator_pid = pid
    end

    def wakeup!
      return unless @wakeup

      begin
        @wakeup.write "!" unless @wakeup.closed?
      rescue SystemCallError, IOError
        Thread.current.purge_interrupt_queue if Thread.current.respond_to? :purge_interrupt_queue
      end
    end

    def restart
      @restart = true
      stop
    end

    def phased_restart
      return false if @options[:preload_app]

      @phased_restart = true
      wakeup!

      true
    end

    def stop
      @status = :stop
      wakeup!
    end

    def stop_blocked
      @status = :stop if @status == :run
      wakeup!
      @control.stop(true) if @control
      Process.waitall
    end

    def halt
      @status = :halt
      wakeup!
    end

    def reload_worker_directory
      dir = @launcher.restart_dir
      log "+ Changing to #{dir}"
      Dir.chdir dir
    end

    # Inside of a child process, this will return all zeroes, as @workers is only populated in
    # the master process.
    def stats
      old_worker_count = @workers.count { |w| w.phase != @phase }
      worker_status = @workers.map do |w|
        {
          started_at: w.started_at.utc.iso8601,
          pid: w.pid,
          index: w.index,
          phase: w.phase,
          booted: w.booted?,
          last_checkin: w.last_checkin.utc.iso8601,
          last_status: w.last_status,
        }
      end

      {
        started_at: @started_at.utc.iso8601,
        workers: @workers.size,
        phase: @phase,
        booted_workers: worker_status.count { |w| w[:booted] },
        old_workers: old_worker_count,
        worker_status: worker_status,
      }
    end

    # We do this in a separate method to keep the lambda scope
    # of the signals handlers as small as possible.
    def setup_signals
      Signal.trap "SIGCHLD" do
        wakeup!
      end

      Signal.trap "TTIN" do
        @options[:workers] += 1
        wakeup!
      end

      Signal.trap "TTOU" do
        @options[:workers] -= 1 if @options[:workers] >= 2
        wakeup!
      end

      master_pid = Process.pid

      Signal.trap "SIGTERM" do
        # The worker installs their own SIGTERM when booted.
        # Until then, this is run by the worker and the worker
        # should just exit if they get it.
        if Process.pid != master_pid
          log "Early termination of worker"
          exit! 0
        else
          @launcher.close_binder_listeners

          stop_worker_generators
          stop

          raise(SignalException, "SIGTERM") if @options[:raise_exception_on_sigterm]
          exit 0 # Clean exit, workers were stopped
        end
      end
    end

    def check_worker_generators(force=false)
      return if !force && @next_check && @next_check >= Time.now

      @next_check = Time.now + Const::WORKER_CHECK_INTERVAL

      if @phase != @current_worker_generator.phase
        if @current_worker_generator.num_desired_workers == 0 && @next_worker_generator.num_desired_workers == @options[:workers]
          log 'killing current worker generator, swapping'
          Process.kill 'TERM', @current_worker_generator_pid
        else
          new_workers = if @next_worker_generator
                          @next_worker_generator.num_desired_workers
                        else
                          0
                        end
          total_workers = @current_worker_generator.num_desired_workers + new_workers

          if total_workers >= @options[:workers]
            log 'decreasing number of desired workers on current worker generator'
            @current_worker_generator.num_desired_workers -= 1
            Process.kill 'TTOU', @current_worker_generator_pid
          else
            log 'increasing number of desired workers on new worker generator'
            if @next_worker_generator_pid
              @next_worker_generator.num_desired_workers += 1
              Process.kill 'TTIN', @next_worker_generator_pid
            else
              @next_worker_generator = WorkerGenerator.new phase: @phase,
                                                           num_desired_workers: 1,
                                                           options: @options,
                                                           launcher: @launcher,
                                                           events: @events
              pid = fork { @next_worker_generator.run }

              if !pid
                log "! Complete inability to spawn new worker generator detected"
                log "! Seppuku is the only choice."
                exit! 1
              end

              debug "Spawned worker generator: #{pid}"

              @next_worker_generator_pid = pid
            end
          end
        end
      end

      wait_worker_generators
    end

    def run
      @status = :run

      output_header "cluster"

      log "* Process workers: #{@options[:workers]}"

      log "* Phased restart available"

      unless @launcher.config.app_configured?
        error "No application configured, nothing to run"
        exit 1
      end

      @launcher.binder.parse @options[:binds], self

      read, @wakeup = Puma::Util.pipe

      setup_signals

      # Used by the workers to detect if the master process dies.
      # If select says that @check_pipe is ready, it's because the
      # master has exited and @suicide_pipe has been automatically
      # closed.
      #
      @check_pipe, @suicide_pipe = Puma::Util.pipe

      if daemon?
        log "* Daemonizing..."
        Process.daemon(true)
      else
        log "Use Ctrl-C to stop"
      end

      redirect_io

      Plugins.fire_background

      @launcher.write_state

      start_control

      @master_read, @worker_write = read, @wakeup

      @launcher.config.run_hooks :before_fork, nil
      GC.compact if GC.respond_to?(:compact)

      spawn_initial_worker_generator

      Signal.trap "SIGINT" do
        stop
      end

      @launcher.events.fire_on_booted!

      begin
        force_check = false

        while @status == :run
          begin
            if @phased_restart
              start_phased_restart
              @phased_restart = false
            end

            check_worker_generators force_check

            force_check = false

            # TODO: check for messages from worker generators
            sleep Const::WORKER_CHECK_INTERVAL
          rescue Interrupt
            @status = :stop
          end
        end

        stop_worker_generators unless @status == :halt
      ensure
        @check_pipe.close
        @suicide_pipe.close
        read.close
        @wakeup.close
      end
    end

    private

    # loops thru @workers, removing workers that exited, and calling
    # `#term` if needed
    def wait_worker_generators
      remove_current = begin
                         if @current_worker_generator_pid && Process.wait(@current_worker_generator_pid, Process::WNOHANG)
                           true
                         end
                       rescue Errno::ECHILD
                         true # child is already terminated
                       end
      remove_next = begin
                      if @next_worker_generator_pid && Process.wait(@next_worker_generator_pid, Process::WNOHANG)
                        true
                      end
                    rescue Errno::ECHILD
                      true # child is already terminated
                    end
      if remove_current
        @current_worker_generator = @next_worker_generator
        @current_worker_generator_pid = @next_worker_generator_pid
        @next_worker_generator = @next_worker_generator_pid = nil
      end
      if remove_next
        @next_worker_generator_pid = @next_worker_generator = nil
      end
    end
  end
end
