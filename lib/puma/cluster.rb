# frozen_string_literal: true

require 'puma/worker_generator'

module Puma
  # This class is instantiated by the `Puma::Launcher` and used
  # to boot and serve a Ruby application when puma "workers" are needed
  # i.e. when using multi-processes. For example `$ puma -w 5`
  class Cluster
    attr_reader :worker_generator

    def initialize(cli, events)
      @worker_generator = WorkerGenerator.new cli, events
    end

    def redirect_io
      raise NotImplementedError
    end

    def before_restart
      worker_generator.before_restart
    end

    def restart
      worker_generator.restart
    end

    def phased_restart
      worker_generator.phased_restart
    end

    def close_control_listeners
      worker_generator.close_control_listeners
    end

    def stop
      worker_generator.stop
    end

    def stop_blocked
      worker_generator.stop_blocked
    end

    def halt
      raise NotImplementedError
    end

    def reload_worker_directory
      raise NotImplementedError
    end

    def stats
      worker_generator.stats
    end

    def run
      worker_generator.run
    end
  end
end
