module DbCharmer
  module ActiveRecord
    module MultiDbProxy
      # Simple proxy class that switches connections and then proxies all the calls
      # This class is used to implement chained on_db calls
      class OnDbProxy < ActiveSupport::BasicObject
        # We need to do this because in Rails 2.3 BasicObject does not remove object_id method, which is stupid
        undef_method(:object_id) if instance_methods.member?('object_id')

        def initialize(proxy_target, slave)
          @proxy_target = proxy_target
          @slave = slave
        end

      private

        def method_missing(meth, *args, &block)
          # Switch connection and proxy the method call
          @proxy_target.on_db(@slave) do |proxy_target|
            res = proxy_target.__send__(meth, *args, &block)

            # If result is a scope/association, return a new proxy for it, otherwise return the result itself
            (res.proxy?) ? OnDbProxy.new(res, @slave) : res
          end
        end
      end

      module ClassMethods
        def on_db(con, proxy_target = nil)
          proxy_target ||= self

          # Chain call
          return OnDbProxy.new(proxy_target, con) unless block_given?

          # Block call
          begin
            self.db_charmer_connection_level += 1
            old_proxy = db_charmer_connection_proxy
            switch_connection_to(con, DbCharmer.connections_should_exist?)
            yield(proxy_target)
          ensure
            switch_connection_to(old_proxy)
            self.db_charmer_connection_level -= 1
          end
        end
      end

      module InstanceMethods
        def on_db(con, proxy_target = nil, &block)
          proxy_target ||= self
          self.class.on_db(con, proxy_target, &block)
        end
      end

      module MasterSlaveClassMethods
        def on_slave(con = nil, proxy_target = nil, &block)
          raise ArgumentError, "No slaves found in the class and no slave connection given" if db_charmer_slaves.empty?

          con ||= begin
            db_charmer_random_live_slave
          rescue
            nil
          end

          return on_master(proxy_target, &block) if con.nil?

          begin
            on_db(con, proxy_target, &block)
          rescue ::ActiveRecord::ActiveRecordError => e
            # First, report that things are probably not going so well.
            if defined?(NewRelic) && NewRelic::Agent.instance_eval{@agent}
              # AFAICT, the only way to find out if the NR agent is running without raising an
              # exception is to check on the @agent ivar. :'(
              NewRelic::Agent.notice_error(e)
            end
            if defined?(Wanelo::Metrics)
              Wanelo::Metrics.instance.increment("errors.db.slave_query")
            end
            Rails.logger.warn "#{e.class}: #{e.message}\n#{e.backtrace.join('\n  ')}"

            # Then, record the slave failure time, and try another option
            db_charmer_slaves_failed_at[con.connection_name] = Time.now.to_i

            live_slaves = db_charmer_live_slaves
            if live_slaves.empty?
              return on_master(proxy_target, &block)
            else
              con = live_slaves[rand(live_slaves.size)]
              retry
            end
          end
        end

        def on_master(proxy_target = nil, &block)
          on_db(db_charmer_default_connection, proxy_target, &block)
        end

        def first_level_on_slave
          first_level = db_charmer_top_level_connection? && on_master.connection.open_transactions.zero?
          if first_level && db_charmer_force_slave_reads? && db_charmer_slaves.any?
            on_slave { yield }
          else
            yield
          end
        end
      end
    end
  end
end
