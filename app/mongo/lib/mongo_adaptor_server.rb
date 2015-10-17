require 'mongo'

# We need to be able to deeply stringify keys for mongo
class Hash
  def nested_stringify_keys
    self.stringify_keys.map do |key, value|
      if value.is_a?(Hash)
        value = value.nested_stringify_keys
      end

      [key, value]
    end.to_h
  end
end

module Volt
  class DataStore
    class MongoAdaptorServer < BaseAdaptorServer
      attr_reader :db, :mongo_db

      # check if the database can be connected to.
      # @return Boolean
      def connected?
        begin
          db

          true
        rescue ::Mongo::Error => e
          false
        end
      end

      def db
        return @db if @db

        if Volt.config.db_uri.present?
          db_name = Volt.config.db_uri.split('/').last || Volt.config.db_name
          @db ||= ::Mongo::Client.new(Volt.config.db_uri, database: db_name, :monitoring => false)
        else
          db_name = Volt.config.db_name
          @db ||= ::Mongo::Client.new("mongodb://#{Volt.config.db_host}:#{Volt.config.db_port}", database: db_name, :monitoring => false)
        end

        @db
      end

      def insert(collection, values)
        db[collection].insert_one(values)
      end

      def update(collection, values)
        values = values.nested_stringify_keys

        to_mongo_id!(values)
        # TODO: Seems mongo is dumb and doesn't let you upsert with custom id's
        begin
          db[collection].insert_one(values)
        rescue => error
          # Really mongo client?
          if error.message[/^E11000/] && error.message['$_id_']
            # Update because the id already exists
            update_values = values.dup
            id = update_values.delete('_id')
            db[collection].update_one({ '_id' => id }, update_values)
          else
            return { error: error.message }
          end
        end

        nil
      end

      def query(collection, query)
        if ENV['DB_LOG'] && collection.to_s != 'active_volt_instances'
          Volt.logger.info("Query: #{collection}: #{query.inspect}")
        end

        allowed_methods = %w(find skip limit sort)

        result = db[collection]

        query.each do |query_part|
          method_name, *args = query_part

          unless allowed_methods.include?(method_name.to_s)
            fail "`#{method_name}` is not part of a valid query"
          end

          args = args.map do |arg|
            if arg.is_a?(Hash)
              arg = arg.stringify_keys
            end
            arg
          end

          if method_name == 'find' && args.size > 0
            qry = args[0]
            to_mongo_id!(qry)
          end

          result = result.send(method_name, *args)
        end

        if result.is_a?(::Mongo::Collection::View)
          result = result.to_a.map do |hash|
            # Return id instead of _id
            to_volt_id!(hash)

            # Volt expects symbol keys
            hash.symbolize_keys
          end#.tap {|v| puts "QUERY: " + v.inspect }
        end

        result
      end

      def delete(collection, query)
        if query.key?('id')
          query['_id'] = query.delete('id')
        end

        db[collection].delete_one(query)
      end

      # remove the collection entirely
      def drop_collection(collection)
        db[collection].drop
      end

      def drop_database
        db.database.drop
      end

      def adapter_version
        ::Mongo::VERSION
      end

      private
      # Mutate a hash to use id instead of _id
      def to_volt_id!(hash)
        if hash.key?('_id')
          # Run to_s to convert BSON::Id also
          hash['id'] = hash.delete('_id').to_s
        end
      end

      # Mutate a hash to use _id instead of id
      def to_mongo_id!(hash)
        if hash.key?('id')
          hash['_id'] = hash.delete('id')
        end
      end

    end
  end
end
