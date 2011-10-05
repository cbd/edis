-module(connection_SUITE).
-compile(export_all).

all() ->
	[auth,
	 echo,
	 ping,
	 select,
	 quit].

init_per_suite(Config) ->
	{ok,Client} = connect_erldis(10),
	NewConfig=[{client,Client}|Config],
	NewConfig.

connect_erldis(0) -> {error,{socket_error,econnrefused}};
connect_erldis(Times) ->
	timer:sleep(2000),
	case erldis:connect(localhost,16380) of
		{ok,Client} -> {ok,Client};
		_ -> connect_erldis(Times - 1)
	end.

end_per_suite(Config) ->
	Config.

auth(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	Pwd = <<"hhedls56329">>,
	
	[ok] = erldis_client:scall(Client, [<<"config">>,<<"set">>,<<"requirepass">>, Pwd]),
	{error,<<"ERR operation not permitted">>} = erldis_client:sr_scall(Client, <<"ping">>),
	
	[{error,<<"ERR invalid password">>}] = erldis_client:scall(Client, [<<"auth">>, <<"bad_pass">>]),
	{error,<<"ERR operation not permitted">>} = erldis_client:sr_scall(Client, <<"ping">>),
	
	[ok] = erldis_client:scall(Client, [<<"auth">>, Pwd]),
	pong = erldis_client:sr_scall(Client, <<"ping">>).

echo(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	Msg = <<"echo this message">>,
	Msg = erldis_client:sr_scall(Client, [<<"echo">>,Msg]).

ping(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	pong = erldis_client:sr_scall(Client, <<"ping">>).

select(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	ok = erldis_client:sr_scall(Client, [<<"select">>, 2]),
	ok = erldis_client:sr_scall(Client, [<<"select">>, 0]).

quit(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	shutdown = erldis_client:stop(Client).
	