# Place any code you want to run when the component is included on the client
# or server.

# To include code only on the client use:
# if RUBY_PLATFORM == 'opal'
#
# To include code only on the server, use:
# unless RUBY_PLATFORM == 'opal'
# ^^ this will not send compile in code in the conditional to the client.
# ^^ this include code required in the conditional.
require 'mongo/lib/mongo_adaptor_client'
if RUBY_PLATFORM != 'opal'
  require 'mongo/lib/mongo_adaptor_server'
end
