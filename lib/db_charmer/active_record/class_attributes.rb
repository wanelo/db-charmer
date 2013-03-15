module DbCharmer
  module ActiveRecord
    module ClassAttributes
      @@db_charmer_opts = {}
      def db_charmer_opts=(opts)
        @@db_charmer_opts[self.name] = opts
      end

      def db_charmer_opts
        @@db_charmer_opts[self.name] || {}
      end

      #---------------------------------------------------------------------------------------------
      @@db_charmer_default_connections = {}
      def db_charmer_default_connection=(conn)
        @@db_charmer_default_connections[self.name] = conn
      end

      def db_charmer_default_connection
        @@db_charmer_default_connections[self.name]
      end

      #---------------------------------------------------------------------------------------------
      @@db_charmer_slaves = {}
      def db_charmer_slaves=(slaves)
        @@db_charmer_slaves[self.name] = slaves
      end

      def db_charmer_slaves
        @@db_charmer_slaves[self.name] || []
      end

      # Returns a random connection from the list of slaves configured for this AR class
      def db_charmer_random_slave
        return nil unless db_charmer_slaves.any?
        db_charmer_slaves[rand(db_charmer_slaves.size)]
      end

      #---------------------------------------------------------------------------------------------
      # Track the last time a request to each slave failed
      def db_charmer_slaves_failed_at
        Thread.current[:db_charmer_slaves_failed_at] ||= {}
      end

      # Return a new array containing only slaves that have not responded to a request
      # with an error in the last 15 seconds
      def db_charmer_live_slaves
        return nil unless db_charmer_slaves.any?
        db_charmer_slaves.select do |s|
          failed_at = db_charmer_slaves_failed_at[s.connection_name]
          failed_at.nil? || failed_at < (Time.now.to_i - 15)
        end
      end

      # Returns a random connection from the list of slaves that has not errored recently
      def db_charmer_random_live_slave
        live_slaves = db_charmer_live_slaves
        return nil unless live_slaves.any?
        live_slaves[rand(live_slaves.size)]
      end

      #---------------------------------------------------------------------------------------------
      @@db_charmer_connection_proxies = {}
      def db_charmer_connection_proxies
        Thread.exclusive do
          @@db_charmer_connection_proxies
        end
      end

      def db_charmer_connection_proxy=(proxy)
        Thread.exclusive do
          @@db_charmer_connection_proxies[self.name] = proxy
        end
      end

      def db_charmer_connection_proxy
        db_charmer_connection_proxies[self.name]
      end

      #---------------------------------------------------------------------------------------------
      @@db_charmer_force_slave_reads_flags = {}
      def db_charmer_force_slave_reads_flags
        Thread.current[:db_charmer_force_slave_reads_flags] ||= @@db_charmer_force_slave_reads_flags
      end

      def db_charmer_force_slave_reads=(force)
        Thread.exclusive do
          db_charmer_force_slave_reads_flags[self.name] = force
        end
      end

      def db_charmer_force_slave_reads
        db_charmer_force_slave_reads_flags[self.name]
      end

      # Slave reads are used in two cases:
      #  - per-model slave reads are enabled (see db_magic method for more details)
      #  - global slave reads enforcing is enabled (in a controller action)
      def db_charmer_force_slave_reads?
        db_charmer_force_slave_reads || DbCharmer.force_slave_reads?
      end

      #---------------------------------------------------------------------------------------------
      @@db_charmer_connection_levels = Hash.new(0)
      def db_charmer_connection_levels
        @@db_charmer_connection_levels
      end

      def db_charmer_connection_level=(level)
        Thread.exclusive { db_charmer_connection_levels[self.name] = level }
      end

      def db_charmer_connection_level
        db_charmer_connection_levels[self.name] || 0
      end

      def db_charmer_top_level_connection?
        db_charmer_connection_level.zero?
      end

      #---------------------------------------------------------------------------------------------
      def db_charmer_remapped_connection
        return nil unless db_charmer_top_level_connection?

        proxy = db_charmer_connection_proxy
        if proxy && proxy.is_a?(DbCharmer::ConnectionProxy)
          name = proxy.db_charmer_connection_name.to_sym
        end

        name ||= :master
        remapped = db_charmer_database_remappings[name]
        remapped ? DbCharmer::ConnectionFactory.connect(remapped, true) : nil
      end

      def db_charmer_database_remappings
        Thread.current[:db_charmer_database_remappings] ||= Hash.new
      end

      def db_charmer_database_remappings=(mappings)
        raise "Mappings must be nil or respond to []" if mappings && (! mappings.respond_to?(:[]))
        Thread.current[:db_charmer_database_remappings] = mappings || {}
      end
    end
  end
end
