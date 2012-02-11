# Gearman Plugin for Server Density

Gets information about a gearman queue. Requires Python 2.6 because of
`socket.create_connection`, `bytearray` and probably a few other bits
and pieces.

## Example Configuration

You must specify the gearman_server config parameter in your
sd-agent.conf (probably `/etc/sd-agent/sd-agent.conf`). If you don't,
the plugin won't do anything (so that you can disable the plugin via
config).

The port and timeout should be fine by default, and you don't need to
specify them.

```
gearman_server:  localhost
#gearman_port:    4730
#gearman_timeout: 5

## Output Keys

The plugin returns a bunch of output keys:

  - `gearman_%FUNCTION_NAME%_workers` - The total number of workers
      capable of running that function
  - `gearman_%FUNCTION_NAME%_running` - The number of this job currently
      being run by workers
  - `gearman_%FUNCTION_NAME%_queue` - The number of this job on the
      queue
  - `gearman_total_workers`, `gearman_total_running`,
      `gearman_total_queue`: as above, but totals across all functions.

Unfortunately, because the names vary, these are rather hard to set up
in seperate Server Density graphs. You might be better off using a
single graph and just toggling them on and off.
