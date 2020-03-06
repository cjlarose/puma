# frozen_string_literal: true

require 'puma/runner'
require 'puma/util'
require 'puma/plugin'

require 'time'

module Puma
  class WorkerGenerator < Runner
    attr_reader :phase
    attr_accessor :num_desired_workers

    def initialize(phase:, num_desired_workers:, options:, launcher:, events:)
      super launcher, events

      @phase = phase
      @num_desired_workers = num_desired_workers
      @options = options
      @workers = []
      @next_check = nil
    end

    def stop_workers
      log "- Gracefully shutting down workers..."
      @workers.each { |x| x.term }

      begin
        loop do
          wait_workers
          break if @workers.empty?
          sleep 0.2
        end
      rescue Interrupt
        log "! Cancelled waiting for workers"
      end
    end

    class Worker
      def initialize(idx, pid, phase, options)
        @index = idx
        @pid = pid
        @phase = phase
        @stage = :started
        @signal = "TERM"
        @options = options
        @first_term_sent = nil
        @started_at = Time.now
        @last_checkin = Time.now
        @last_status = {}
        @term = false
      end

      attr_reader :index, :pid, :phase, :signal, :last_checkin, :last_status, :started_at

      def booted?
        @stage == :booted
      end

      def boot!
        @last_checkin = Time.now
        @stage = :booted
      end

      def term?
        @term
      end

      def ping!(status)
        @last_checkin = Time.now
        captures = status.match(/{ "backlog":(?<backlog>\d*), "running":(?<running>\d*), "pool_capacity":(?<pool_capacity>\d*), "max_threads": (?<max_threads>\d*), "requests_count": (?<requests_count>\d*) }/)
        @last_status = captures.names.inject({}) do |hash, key|
          hash[key.to_sym] = captures[key].to_i
          hash
        end
      end

      def ping_timeout?(which)
        Time.now - @last_checkin > which
      end

      def term
        begin
          if @first_term_sent && (Time.now - @first_term_sent) > @options[:worker_shutdown_timeout]
            @signal = "KILL"
          else
            @term ||= true
            @first_term_sent ||= Time.now
          end
          Process.kill @signal, @pid
        rescue Errno::ESRCH
        end
      end

      def kill
        Process.kill "KILL", @pid
      rescue Errno::ESRCH
      end

      def hup
        Process.kill "HUP", @pid
      rescue Errno::ESRCH
      end
    end

    def spawn_workers
      diff = @num_desired_workers - @workers.size
      return if diff < 1

      master = Process.pid

      diff.times do
        idx = next_worker_index
        @launcher.config.run_hooks :before_worker_fork, idx

        pid = fork { worker(idx, master) }
        if !pid
          log "! Complete inability to spawn new workers detected"
          log "! Seppuku is the only choice."
          exit! 1
        end

        debug "Spawned worker: #{pid}"
        @workers << Worker.new(idx, pid, @phase, @options)

        @launcher.config.run_hooks :after_worker_fork, idx
      end
    end

    def wakeup!
      return unless @wakeup

      begin
        @wakeup.write "!" unless @wakeup.closed?
      rescue SystemCallError, IOError
        Thread.current.purge_interrupt_queue if Thread.current.respond_to? :purge_interrupt_queue
      end
    end

    def cull_workers
      diff = @workers.size - @num_desired_workers
      return if diff < 1

      debug "Culling #{diff.inspect} workers"

      workers_to_cull = @workers[-diff,diff]
      debug "Workers to cull: #{workers_to_cull.inspect}"

      workers_to_cull.each do |worker|
        log "- Worker #{worker.index} (pid: #{worker.pid}) terminating"
        worker.term
      end
    end

    def next_worker_index
      all_positions =  0...@num_desired_workers
      occupied_positions = @workers.map { |w| w.index }
      available_positions = all_positions.to_a - occupied_positions
      available_positions.first
    end

    def preload?
      @options[:preload_app]
    end

    def setup_signals
      Signal.trap "SIGTERM" do
        stop_workers

        exit 0
      end

      Signal.trap "TTIN" do
        @num_desired_workers += 1
        wakeup!
      end

      Signal.trap "TTOU" do
        @num_desired_workers -= 1
        wakeup!
      end
    end

    def check_workers(force=false)
      return if !force && @next_check && @next_check >= Time.now

      @next_check = Time.now + Const::WORKER_CHECK_INTERVAL

      any = false

      @workers.each do |w|
        next if !w.booted? && !w.ping_timeout?(@options[:worker_boot_timeout])
        if w.ping_timeout?(@options[:worker_timeout])
          log "! Terminating timed out worker: #{w.pid}"
          w.kill
          any = true
        end
      end

      # If we killed any timed out workers, try to catch them
      # during this loop by giving the kernel time to kill them.
      sleep 1 if any

      wait_workers
      cull_workers
      spawn_workers
    end

    def worker(index, master)
      title  = "puma: cluster worker #{index}: #{master}"
      title += " [#{@options[:tag]}]" if @options[:tag] && !@options[:tag].empty?
      $0 = title

      Signal.trap "SIGINT", "IGNORE"

      @workers = []
      @master_read.close
      @suicide_pipe.close

      Thread.new do
        Puma.set_thread_name "worker check pipe"
        IO.select [@check_pipe]
        log "! Detected parent died, dying"
        exit! 1
      end

      # If we're not running under a Bundler context, then
      # report the info about the context we will be using
      if !ENV['BUNDLE_GEMFILE']
        if File.exist?("Gemfile")
          log "+ Gemfile in context: #{File.expand_path("Gemfile")}"
        elsif File.exist?("gems.rb")
          log "+ Gemfile in context: #{File.expand_path("gems.rb")}"
        end
      end

      # Invoke any worker boot hooks so they can get
      # things in shape before booting the app.
      @launcher.config.run_hooks :before_worker_boot, index

      server = start_server

      Signal.trap "SIGTERM" do
        @worker_write << "e#{Process.pid}\n" rescue nil
        server.stop
      end

      begin
        @worker_write << "b#{Process.pid}\n"
      rescue SystemCallError, IOError
        Thread.current.purge_interrupt_queue if Thread.current.respond_to? :purge_interrupt_queue
        STDERR.puts "Master seems to have exited, exiting."
        return
      end

      Thread.new(@worker_write) do |io|
        Puma.set_thread_name "stat payload"
        base_payload = "p#{Process.pid}"

        while true
          sleep Const::WORKER_CHECK_INTERVAL
          begin
            b = server.backlog || 0
            r = server.running || 0
            t = server.pool_capacity || 0
            m = server.max_threads || 0
            rc = server.requests_count || 0
            payload = %Q!#{base_payload}{ "backlog":#{b}, "running":#{r}, "pool_capacity":#{t}, "max_threads": #{m}, "requests_count": #{rc} }\n!
            io << payload
          rescue IOError
            Thread.current.purge_interrupt_queue if Thread.current.respond_to? :purge_interrupt_queue
            break
          end
        end
      end

      server.run.join

      # Invoke any worker shutdown hooks so they can prevent the worker
      # exiting until any background operations are completed
      @launcher.config.run_hooks :before_worker_shutdown, index
    ensure
      @worker_write << "t#{Process.pid}\n" rescue nil
      @worker_write.close
    end

    def preload!
      return unless preload?

      before = Thread.list

      log "* Preloading application"
      load_application

      after = Thread.list

      if after.size > before.size
        threads = (after - before)
        if threads.first.respond_to? :backtrace
          log "! WARNING: Detected #{after.size-before.size} Thread(s) started in app boot:"
          threads.each do |t|
            log "! #{t.inspect} - #{t.backtrace ? t.backtrace.first : ''}"
          end
        else
          log "! WARNING: Detected #{after.size-before.size} Thread(s) started in app boot"
        end
      end
    end

    def run
      title  = "puma: cluster worker generator for phase #{@phase}"
      title += " [#{@options[:tag]}]" if @options[:tag] && !@options[:tag].empty?
      $0 = title

      dir = @launcher.restart_dir
      log "+ Changing to #{dir}"
      Dir.chdir dir

      Signal.trap "SIGINT", "IGNORE"

      @status = :run

      log "Starting worker generator in phase #{@phase}"
      preload!

      read, @wakeup = Puma::Util.pipe

      setup_signals

      # Used by the workers to detect if the worker generator process dies.
      # If select says that @check_pipe is ready, it's because the
      # master has exited and @suicide_pipe has been automatically
      # closed.
      #
      @check_pipe, @suicide_pipe = Puma::Util.pipe

      @master_read, @worker_write = read, @wakeup

      spawn_workers

      begin
        force_check = false

        while @status == :run
          begin
            check_workers force_check

            force_check = false

            res = IO.select([read], nil, nil, Const::WORKER_CHECK_INTERVAL)

            if res
              req = read.read_nonblock(1)

              next if !req || req == "!"

              result = read.gets
              pid = result.to_i

              if w = @workers.find { |x| x.pid == pid }
                case req
                when "b"
                  w.boot!
                  log "- Worker #{w.index} (pid: #{pid}) booted, phase: #{w.phase}"
                  force_check = true
                when "e"
                  # external term, see worker method, Signal.trap "SIGTERM"
                  w.instance_variable_set :@term, true
                when "t"
                  w.term unless w.term?
                  force_check = true
                when "p"
                  w.ping!(result.sub(/^\d+/,'').chomp)
                end
              else
                log "! Out-of-sync worker list, no #{pid} worker"
              end
            end

          rescue Interrupt
            @status = :stop
          end
        end

        stop_workers unless @status == :halt
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
    def wait_workers
      @workers.reject! do |w|
        begin
          if Process.wait(w.pid, Process::WNOHANG)
            true
          else
            w.term if w.term?
            nil
          end
        rescue Errno::ECHILD
          true # child is already terminated
        end
      end
    end
  end
end
