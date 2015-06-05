module Volt
  class DataStore
    class MongoAdaptorClient < BaseAdaptorClient
      data_store_methods :find, :where, :skip, :order, :limit#, :count

      module MongoArrayStore
        # Find takes a query object
        def where(query = {})
          add_query_part(:find, query)
        end
        alias_method :find, :where

        # .sort is already a ruby method, so we use order instead
        def order(sort)
          add_query_part(:sort, sort)
        end

        # def count
        #   add_query_part(:count).then
        # end
      end

      # Due to the way define_method works, we need to remove the generated
      # methods from data_store_methods before we over-ride them.
      Volt::Persistors::ArrayStore.send(:remove_method, :where)
      Volt::Persistors::ArrayStore.send(:remove_method, :order)
      # Volt::Persistors::ArrayStore.send(:remove_method, :count)

      # include mongo's methods on ArrayStore
      Volt::Persistors::ArrayStore.send(:include, MongoArrayStore)

      def self.normalize_query(query)
        query = merge_finds_and_move_to_front(query)

        query = reject_skip_zero(query)

        query
      end

      def self.merge_finds_and_move_to_front(query)
        # Map first parts to string
        query = query.map { |v| v[0] = v[0].to_s; v }
        has_find = query.find { |v| v[0] == 'find' }

        if has_find
          # merge any finds
          merged_find_query = {}
          query = query.reject do |query_part|
            if query_part[0] == 'find'
              # on a find, merge into finds
              find_query = query_part[1]
              merged_find_query.merge!(find_query) if find_query

              # reject
              true
            else
              false
            end
          end

          # Add finds to the front
          query.insert(0, ['find', merged_find_query])
        else
          # No find was done, add it in the first position
          query.insert(0, ['find'])
        end

        query
      end

      def self.reject_skip_zero(query)
        query.reject do |query_part|
          query_part[0] == 'skip' && query_part[1] == 0
        end
      end

    end
  end
end