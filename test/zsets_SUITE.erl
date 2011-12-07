%% @hidden
-module(zsets_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(ERR_NUM_ARGS(Command), {error,<<"ERR wrong number of arguments for '",Command/binary,"' command">>}).
-define(ERR_NOTDOUBLE, {error,<<"ERR value is not a double">>}).
-define(ERR_SINTAX, {error,<<"ERR syntax error">>}).

all() -> [zadd,zincrby,zcard].

init_per_suite(Config) ->
	{ok,Client} = connect_erldis(10),
	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

init_per_testcase(_TestCase,Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	erldis_client:sr_scall(Client,[<<"flushdb">>]),
	Config.

end_per_suite(_Config) ->
	ok.

connect_erldis(0) -> {error,{socket_error,econnrefused}};
connect_erldis(Times) ->
	timer:sleep(2000),
	case erldis:connect(localhost,16380) of
		{ok,Client} -> {ok,Client};
		_ -> connect_erldis(Times - 1)
	end.

zadd(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	%% basic ZADD and score update
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,10,<<"x">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,20,<<"y">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,30,<<"z">>]),
	[<<"x">>,<<"y">>,<<"z">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-1]),
	false = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,1,<<"y">>]),
	[<<"y">>,<<"x">>,<<"z">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-1]),
	%% element can't be set to NaN with ZADD
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,<<"nan">>,<<"x">>]),
	%% ZADD - Variadic version base case
	3 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,10,<<"x">>,20,<<"y">>,30,<<"z">>]),
	[<<"x">>,<<"10">>,<<"y">>,<<"20">>,<<"z">>,<<"30">>] = erldis_client:scall(Client,[<<"zrange">>,<<"myset">>,0,-1,<<"withscores">>]),
	%% ZADD - Return value is the number of actually added items
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,5,<<"a">>,20,<<"y">>,30,<<"z">>]),
	[<<"a">>,<<"5">>,<<"x">>,<<"10">>,<<"y">>,<<"20">>,<<"z">>,<<"30">>] = erldis_client:scall(Client,[<<"zrange">>,<<"myset">>,0,-1,<<"withscores">>]),
	%% ZADD - Variadic version does not add nothing on single parsing err
	true = erldis_client:sr_scall(Client,[<<"del">>,<<"myset">>]),
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,10,<<"x">>,20,<<"y">>,<<"thirty">>,<<"z">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"myset">>]),
	%% ZADD - Variadic version will raise error on missing arg
	?ERR_SINTAX = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset2">>,10,<<"x">>,20,<<"y">>,30]),
	ok.

zincrby(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZINCRBY">>),
	%% element can't be set to NaN with ZINCRBY
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"ztmp">>,<<"nan">>,<<"x">>]),
	%% ZINCRBY does not work variadic 
  ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"myset">>,10,<<"x">>,20,<<"y">>,30,<<"z">>]),
	ok.

zcard(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	3 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,10,<<"x">>,20,<<"y">>,30,<<"z">>]),
	3 = erldis_client:sr_scall(Client,[<<"zcard">>,<<"myset">>]),
  false = erldis_client:sr_scall(Client,[<<"zcard">>,<<"otherset">>]),
	ok.