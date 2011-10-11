-module(connection_SUITE).
-compile(export_all).

all() ->
	[echo,ping,select,auth,quit].

init_per_testcase(_TestCase, Config) ->
	{ok,Client} = connect_erldis(10),
	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

connect_erldis(0) -> {error,{socket_error,econnrefused}};
connect_erldis(Times) ->
	timer:sleep(2000),
	case erldis:connect(localhost,16380) of
		{ok,Client} -> {ok,Client};
		_ -> connect_erldis(Times - 1)
	end.

echo(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	Msg = <<"echo this message">>,
	Msg = erldis_client:sr_scall(Client, [<<"echo">>,Msg]).

ping(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	pong = erldis_client:sr_scall(Client, <<"ping">>).

select(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	ok = erldis_client:sr_scall(Client, [<<"select">>, 15]),
	{error,<<"ERR invalid DB index">>} = erldis_client:sr_scall(Client, [<<"select">>, 16]),
	{error,<<"ERR wrong number of arguments for 'SELECT' command">>} = erldis_client:sr_scall(Client, [<<"select">>, 4, 3]),
	{error,<<"ERR invalid DB index">>} = erldis_client:sr_scall(Client, [<<"select">>, -1]),
	ok = erldis_client:sr_scall(Client, [<<"select">>, a]).

auth(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	Pwd = <<"hhedls56329">>,
	
	[ok] = erldis_client:scall(Client, [<<"config">>,<<"set">>,<<"requirepass">>, Pwd]),
	{ok,NewClient} = connect_erldis(10), 
	{error,<<"ERR operation not permitted">>} = erldis_client:sr_scall(NewClient, <<"ping">>),
	
	[{error,<<"ERR invalid password">>}] = erldis_client:scall(NewClient, [<<"auth">>, <<"bad_pass">>]),
	{error,<<"ERR operation not permitted">>} = erldis_client:sr_scall(NewClient, <<"ping">>),
	
	[ok] = erldis_client:scall(NewClient, [<<"auth">>, Pwd]),
	pong = erldis_client:sr_scall(NewClient, <<"ping">>).

quit(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	shutdown = erldis_client:stop(Client).
	