require_relative "helper"
require_relative "helpers/integration"

class TestPlugin < TestIntegration
  def test_plugin
    skip "Skipped on Windows Ruby < 2.5.0, Ruby bug" if windows? && RUBY_VERSION < '2.5.0'
    @tcp_bind = UniquePort.call
    @tcp_ctrl = UniquePort.call

    Dir.mkdir("tmp") unless Dir.exist?("tmp")

    cli_server "-b tcp://#{HOST}:#{@tcp_bind} --control-url tcp://#{HOST}:#{@tcp_ctrl} --control-token #{TOKEN} -C test/config/plugin1.rb test/rackup/hello.ru"
    tmp_file = Tempfile.new 'newserver'
    Process.spawn "tee #{tmp_file.path}", in: @server, out: STDOUT
    @server = tmp_file

    File.open('tmp/restart.txt', mode: 'wb') { |f| f.puts "Restart #{Time.now}" }

    puts "[test_plugin] waiting for Restarting"
    true while (l = @server.gets) !~ /Restarting\.\.\./
    assert_match(/Restarting\.\.\./, l)

    puts "[test_plugin] waiting for Ctrl-C"
    true while (l = @server.gets) !~ /Ctrl-C/
    assert_match(/Ctrl-C/, l)

    out = StringIO.new

    puts "[test_plugin] sending stop"
    cli_pumactl "-C tcp://#{HOST}:#{@tcp_ctrl} -T #{TOKEN} stop"
    puts "[test_plugin] waiting for Goodbye"
    true while (l = @server.gets) !~ /Goodbye/

    puts "[test_plugin] closing server stream"
    @server.close
    @server = nil
    out.close
  end

  private

  def cli_pumactl(argv)
    pumactl = IO.popen("#{BASE} bin/pumactl #{argv}", "r")
    @ios_to_close << pumactl
    Process.wait pumactl.pid
    pumactl
  end
end
