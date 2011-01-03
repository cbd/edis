task :default => [:make_erl]

task :clean do
  print "Cleaning..."
  sh "rebar clean"
  print " done\n"
end

task :make_erl do
  print "Compiling Erlang sources..."
  sh "rebar compile"
  print " done\n"
end

task :run do
  sh "erl -pa ebin -pa src -s crypto -boot start_sasl +Bc +K true -smp enable -run edis_app boot"
end

