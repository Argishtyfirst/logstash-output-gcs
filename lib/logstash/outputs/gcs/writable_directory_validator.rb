# encoding: utf-8
# ---validated for gcs---
module LogStash
  module Outputs
    class GCS
      class WritableDirectoryValidator
        def self.valid?(path)
          begin
            FileUtils.mkdir_p(path) unless Dir.exist?(path)
            ::File.writable?(path)
          rescue
            false
          end
        end
      end
    end
  end
end
