# This is patch related to the autoloading and ruby
#
# The fix exist in jruby 9k but not in the current jruby, not sure when or it will be backported
# https://github.com/jruby/jruby/issues/3645
#
old_stderr = $stderr

$stderr = StringIO.new
begin
  module Gcp
    const_set(:GCS, Gcp::GCS)
  end
ensure
  $stderr = old_stderr
end


