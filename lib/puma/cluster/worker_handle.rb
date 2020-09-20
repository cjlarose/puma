# frozen_string_literal: true

module Puma
  class Cluster < Runner
    class WorkerHandle
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

      # @version 5.0.0
      attr_writer :pid, :phase

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
        require 'json'
        @last_status = JSON.parse(status, symbolize_names: true)
      end

      # @see Puma::Cluster#check_workers
      # @version 5.0.0
      def ping_timeout
        @last_checkin +
          (booted? ?
            @options[:worker_timeout] :
            @options[:worker_boot_timeout]
          )
      end

      def term
        begin
          if @first_term_sent && (Time.now - @first_term_sent) > @options[:worker_shutdown_timeout]
            @signal = "KILL"
          else
            @term ||= true
            @first_term_sent ||= Time.now
          end
          Process.kill @signal, @pid if @pid
        rescue Errno::ESRCH
        end
      end

      def kill
        @signal = 'KILL'
        term
      end

      def hup
        Process.kill "HUP", @pid
      rescue Errno::ESRCH
      end
    end
  end
end