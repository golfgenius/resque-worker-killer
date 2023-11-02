
require "resque/plugins/worker_killer/version"
require 'get_process_mem'

module Resque
  module Plugins
    module WorkerKiller
      def worker_killer_monitor_interval
        @worker_killer_monitor_interval ||= 1.0 # sec
      end

      def worker_killer_agg_monitor_interval
        @worker_killer_agg_monitor_interval || 10.0 #sec
      end

      def worker_killer_agg_mem_limit
        @worker_killer_agg_mem_limit ||= 300 * 1024 * 6 # killo bytes
      end

      def worker_killer_mem_limit
        @worker_killer_mem_limit ||= 300 * 1024 # killo bytes
      end

      def worker_killer_max_term
        @worker_killer_max_term ||= (ENV['TERM_CHILD'] ? 10 : 0)
      end

      def worker_killer_verbose
        @worker_killer_verbose = false if @worker_killer_verbose.nil?
        @worker_killer_verbose
      end

      def worker_killer_logger
        @worker_killer_logger ||= ::Resque.logger
      end

      def self.extended(klass)
        unless klass.respond_to?(:after_perform_logging_killer)
          klass.instance_eval do
            # in case threads act up
            # def after_perform_logging_killer(*args)
            #   Thread.current[:memory_checker_threads]&.each(&:kill)
            # end

            def before_perform_logging_killer(*args)
              # in case threads act up, add the threads to the array and then they'll get killed
              # Thread.current[:memory_checker_threads] ||= []
              Thread.start { PrivateMethods.new(self).monitor_oom }
              Thread.start { PrivateMethods.new(self).monitor_oom(true) }
            end
          end
        end
      end

      class PrivateMethods
        def initialize(obj)
          @obj = obj
        end

        # delegate attr_reader
        %i[
          agg_monitor_interval
          agg_mem_limit
          monitor_interval
          mem_limit
          max_term
          verbose
          logger
        ].each do |method|
          define_method(method) do
            @obj.send("worker_killer_#{method}")
          end
        end

        def plugin_name
          "Resque::Plugins::WorkerKiller"
        end

        def monitor_oom(aggregated = false)
          @log_once = true
          start_time = Time.now
          if aggregated
            loop do
              break if one_shot_agg_monitor_oom(start_time)
              sleep agg_monitor_interval
            end
          else
            loop do
              break if one_shot_monitor_oom(start_time)
              sleep monitor_interval
            end
          end
        end

        def one_shot_agg_monitor_oom(start_time)
          ps_results = `ps -e -o pid,command | grep -E 'resque.*Processing' | grep -v grep`
          worker_pids = ps_results.split(']')[0..-2].map do |rstr|
            rstr.split('resque')[0].to_i
          end
          if @log_once
            ps_results.split('resque').each do |x|
              logger.warn("RESQUE SPLIT -#{x}-")
            end
            @log_once = false
          end
          agg_rss = worker_pids.sum do |pid|
            GetProcessMem.new(pid).kb
          end
          if agg_rss > agg_mem_limit
            logger.warn "Aggregated Memory Sum of workers exceeds memory threshold (#{agg_rss} > #{agg_mem_limit}); PIDS: #{worker_pids}"
            return true
          end
          nil
        end

        def one_shot_monitor_oom(start_time)
          rss = GetProcessMem.new.kb
          logger.info "#{plugin_name}: worker (pid: #{Process.pid}) using #{rss} KB." if verbose
          if rss > mem_limit
            logger.warn "#{plugin_name}: worker (pid: #{Process.pid}) with JOB NAME #{@obj} exceeds memory threshold (#{rss} KB > #{mem_limit} KB)"
            return true
          end
          nil
        end

        # Kill the current process by telling it to send signals to itself If
        # the process isn't killed after `@max_term` TERM signals,
        # send a KILL signal.
        def kill_self(logger, start_time)
          alive_sec = (Time.now - start_time).round

          @@kill_attempts ||= 0
          @@kill_attempts += 1

          sig = :TERM
          sig = :KILL if @@kill_attempts > max_term

          logger.warn "#{plugin_name}: send SIG#{sig} (pid: #{Process.pid}) alive: #{alive_sec} sec (trial #{@@kill_attempts})"
          Process.kill(sig, Process.pid)
        end
      end
    end
  end
end
