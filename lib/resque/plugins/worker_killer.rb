
require "resque/plugins/worker_killer/version"
require 'get_process_mem'

module Resque
  module Plugins
    module WorkerKiller
      # individual job memory checking interval
      def worker_killer_monitor_interval
        @worker_killer_monitor_interval ||= 1.0 # sec
      end

      # aggregate jobs on pod memory checking interval
      def worker_killer_agg_monitor_interval
        @worker_killer_agg_monitor_interval || 10.0 #sec
      end

      # individual job memory limit
      def worker_killer_agg_mem_limit
        @worker_killer_agg_mem_limit ||= 300 * 1024 * 6 # killo bytes
      end

      # aggregate jobs memory limit
      def worker_killer_mem_limit
        @worker_killer_mem_limit ||= 300 * 1024 # killo bytes
      end

      # kind of redundant
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
        unless klass.respond_to?(:before_perform_logging_killer)
          klass.instance_eval do
            # in case threads act up
            # def after_perform_logging_killer(*args)
            #   Thread.current[:memory_checker_threads]&.each(&:kill)
            # end

            # resque is created in such a way that any method prefixed by before_perform gets called before performing the job
            # this one method creates 2 threads, 1 for each case mentioned above, individual and aggregate, and calls the
            # callback_error in case the threads fail
            def before_perform_logging_killer(*args)
              # in case threads act up, add the threads to the array and then they'll get killed
              # Thread.current[:memory_checker_threads] ||= []
              Thread.start do
                begin
                  PrivateMethods.new(self, args).monitor_oom
                rescue Exception => e
                  callback_for_error(e) if respond_to?(:callback_for_error) && method(:callback_for_error).arity == 1
                end
              end
              Thread.start do
                begin
                  PrivateMethods.new(self, args).monitor_oom(true)
                rescue Exception => e
                  callback_for_error(e) if respond_to?(:callback_for_error) && method(:callback_for_error).arity == 1
                end
              end
            end
          end
        end
      end

      class PrivateMethods
        def initialize(obj, args)
          @obj = obj
          @args = args
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

        # infinitely loop in the thread checking the process' memory
        def monitor_oom(aggregated = false)
          if aggregated
            loop do
              break if one_shot_agg_monitor_oom
              sleep agg_monitor_interval
            end
          else
            loop do
              break if one_shot_monitor_oom
              sleep monitor_interval
            end
          end
        end

        # aggregate monitoring, look for all resque jobs processing that are active and get their memory
        def one_shot_agg_monitor_oom
          ps_results = `ps -e -o pid,command | grep -E 'resque.*Processing' | grep -v grep`
          worker_pids = ps_results.split('ResqueLibraries::').map do |rstr|
            rstr.split('resque')[0].scan(/\b\d+\b/).try(:first).to_i
          end
          agg_rss = worker_pids.sum do |pid|
            GetProcessMem.new(pid).kb
          end
          memory_over_threshold?(agg_rss, agg_mem_limit, worker_pids)
        end

        # individual monitoring, get the current process' job and see its memory
        def one_shot_monitor_oom
          rss = GetProcessMem.new.kb
          logger.info "#{plugin_name}: worker (pid: #{Process.pid}) using #{rss} KB." if verbose
          memory_over_threshold?(rss, mem_limit, [])
        end

        # in case the memory is over the predefined threshold, send a warning and call the callback_for_alert_method
        def memory_over_threshold?(rss, mem_limit, worker_pids)
          return unless rss > mem_limit

          alert_msg =
            if worker_pids.present?
              "Aggregated Memory Sum of workers exceeds memory threshold (#{agg_rss} > #{agg_mem_limit}); PIDS: #{worker_pids}\nSource of alert JOB NAME #{@obj} with arguments #{@args}"
            else
              "#{plugin_name}: worker (pid: #{Process.pid}) with JOB NAME #{@obj} and arguments #{@args} exceeds memory threshold (#{rss} KB > #{mem_limit} KB)"
            end
          @obj.callback_for_alert(alert_msg) if @obj.respond_to?(:callback_for_alert) && @obj.method(:callback_for_alert).arity == 1
          logger.warn(alert_msg)
          true
        end
      end
    end
  end
end
