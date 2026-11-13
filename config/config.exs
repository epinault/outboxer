import Config

# Outboxer itself reads no application configuration. This file only exists
# for the library's own test suite.
if config_env() == :test do
  import_config "test.exs"
end
