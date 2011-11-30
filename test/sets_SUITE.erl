%% @hidden
-module(sets_SUITE).
-compile(export_all).

all() ->
	[basic,sadd,srem,sinter,sinterstore,
	 sunion].

init_per_testcase(_TestCase,Config) ->
	{ok,Client} = connect_erldis(10),
    erldis_client:sr_scall(Client,[<<"flushdb">>]),
	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

connect_erldis(0) -> {error,{socket_error,econnrefused}};
connect_erldis(Times) ->
	timer:sleep(2000),
	case erldis:connect(localhost,16380) of
		{ok,Client} -> {ok,Client};
		_ -> connect_erldis(Times - 1)
	end.

-spec create_numeric_sets(pid()) -> ok.
create_numeric_sets(Client) ->
	[{true,true} = 
		 {erldis_client:sr_scall(Client,[<<"sadd">>,<<"set1">>,Int]),
		  erldis_client:sr_scall(Client,[<<"sadd">>,<<"set2">>,Int+195])} 
		 || Int <- lists:seq(0,199)], 
	erldis_client:sr_scall(Client,[<<"sadd">>,<<"set3">>,199,195,1000,2000]),
	ok.

basic(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	%% SADD, SCARD, SISMEMBER, SMEMBERS basics
	%% Regular set
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,<<"foo">>]),
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,<<"bar">>]),
	false = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,<<"bar">>]),
	2 = erldis_client:sr_scall(Client,[<<"scard">>,<<"myset">>]),
	true = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,<<"foo">>]),
	true = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,<<"bar">>]),
	false = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,<<"buzz">>]),
	[<<"bar">>,<<"foo">>] = erldis_client:scall(Client,[<<"smembers">>,<<"myset">>]),
	%% Intset
	true = erldis_client:sr_scall(Client,[<<"del">>,<<"myset">>]),
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,17]),
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,16]),
	false = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,16]),
	2 = erldis_client:sr_scall(Client,[<<"scard">>,<<"myset">>]),
	true = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,16]),
	true = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,17]),
	false = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,18]),
	[<<"16">>,<<"17">>] = erldis_client:scall(Client,[<<"smembers">>,<<"myset">>]).

sadd(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	%% SADD a non-integer against an intset	
	4 = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set1">>,7,8,9,10]),
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set1">>,<<"foo">>]),
	5 = erldis_client:sr_scall(Client,[<<"scard">>,<<"set1">>]),
	%% SADD an integer larger than 64 bits
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set2">>,213244124402402314402033402]),	        
    true = erldis_client:sr_scall(Client,[<<"sismember">>,<<"set2">>,213244124402402314402033402]),
	%% SADD overload with integers
	[true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set3">>,Int]) || Int <- lists:seq(1,1110)],
	1110 = erldis_client:sr_scall(Client,[<<"scard">>,<<"set3">>]),
	%% Variadic SADD
	3 = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set4">>,<<"a">>,<<"b">>,<<"c">>]),
	2 = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set4">>,<<"A">>,<<"a">>,<<"b">>,<<"c">>,<<"B">>]),
    [<<"A">>,<<"B">>,<<"a">>,<<"b">>,<<"c">>] = lists:sort(erldis_client:scall(Client,[<<"smembers">>,<<"set4">>])),
	%% SADD against non set
	true = erldis_client:sr_scall(Client,[<<"lpush">>,<<"list1">>,<<"foo">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"sadd">>,<<"list1">>,<<"bar">>]),
	%% Bad arguments
	{error,<<"ERR wrong number of arguments for 'SADD' command">>}= erldis_client:sr_scall(Client,[<<"sadd">>]),
	{error,<<"ERR wrong number of arguments for 'SADD' command">>}= erldis_client:sr_scall(Client,[<<"sadd">>,<<"set1">>]).

srem(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	%% SREM basics - regular set
	3 = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set1">>,<<"foo">>,<<"bar">>,<<"buzz">>]),
	false = erldis_client:sr_scall(Client,[<<"srem">>,<<"set1">>,<<"fbb">>]),
	true = erldis_client:sr_scall(Client,[<<"srem">>,<<"set1">>,<<"foo">>]),
	[<<"bar">>,<<"buzz">>] = lists:sort(erldis_client:scall(Client,[<<"smembers">>,<<"set1">>])),
	%% SREM basics - intset	
	3 = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set2">>,3,4,5]),
	false = erldis_client:sr_scall(Client,[<<"srem">>,<<"set2">>,6]),
	true = erldis_client:sr_scall(Client,[<<"srem">>,<<"set2">>,4]),
	[<<"3">>,<<"5">>] = lists:sort(erldis_client:scall(Client,[<<"smembers">>,<<"set2">>])),
	%% SREM with multiple arguments
	4 = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set3">>,<<"a">>,<<"b">>,<<"c">>,<<"d">>]),
	false = erldis_client:sr_scall(Client,[<<"srem">>,<<"set3">>,<<"f">>,<<"f">>,<<"f">>,<<"f">>]),
	2 = erldis_client:sr_scall(Client,[<<"srem">>,<<"set3">>,<<"b">>,<<"x">>,<<"d">>,<<"y">>]),
    [<<"a">>,<<"c">>] = lists:sort(erldis_client:scall(Client,[<<"smembers">>,<<"set3">>])),    
	%% SREM variadic version with more args needed to destroy the key
	3 = erldis_client:sr_scall(Client,[<<"sadd">>,<<"set4">>,3,4,5]),
    3 = erldis_client:sr_scall(Client,[<<"srem">>,<<"set4">>,1,2,3,4,5,6,7]),
	%% SREM against non set
	true = erldis_client:sr_scall(Client,[<<"lpush">>,<<"list1">>,<<"foo">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"srem">>,<<"list1">>,<<"bar">>]),
	%% Bad arguments
	{error,<<"ERR wrong number of arguments for 'SREM' command">>}= erldis_client:sr_scall(Client,[<<"srem">>]),
	{error,<<"ERR wrong number of arguments for 'SREM' command">>}= erldis_client:sr_scall(Client,[<<"srem">>,<<"set1">>]).

sinter(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	create_numeric_sets(Client),
	%% SINTER with one set
	[<<"1000">>,<<"195">>,<<"199">>,<<"2000">>] = lists:sort(erldis_client:scall(Client,[<<"sinter">>,<<"set3">>])),
	%% SINTER with two sets
    [<<"195">>,<<"196">>,<<"197">>,<<"198">>,<<"199">>] = lists:sort(erldis_client:scall(Client,[<<"sinter">>,<<"set1">>,<<"set2">>])),
	%% SINTER with three sets
	[<<"195">>,<<"199">>] = lists:sort(erldis_client:scall(Client,[<<"sinter">>,<<"set1">>,<<"set2">>,<<"set3">>])),
	%% SINTER against non-set	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key">>,<<"foo">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"sinter">>,<<"key">>,<<"noset">>]),
	%% Bad Arguments
	{error,<<"ERR wrong number of arguments for 'SINTER' command">>}= erldis_client:sr_scall(Client,[<<"sinter">>]).

sinterstore(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	create_numeric_sets(Client),
	%% SINTERSTORE with two sets
	5 = erldis_client:sr_scall(Client,[<<"sinterstore">>,<<"setres">>,<<"set1">>,<<"set2">>]),
	[<<"195">>,<<"196">>,<<"197">>,<<"198">>,<<"199">>] = lists:sort(erldis_client:scall(Client,[<<"smembers">>,<<"setres">>])),
	%% SINTERSTORE with three sets 
    2 = erldis_client:sr_scall(Client,[<<"sinterstore">>,<<"setres">>,<<"set1">>,<<"set2">>,<<"set3">>]),        
	[<<"195">>,<<"199">>] = lists:sort(erldis_client:scall(Client,[<<"smembers">>,<<"setres">>])),
	%% SINTERSTORE against non existing keys should delete dstkey
	false = erldis_client:sr_scall(Client,[<<"sinterstore">>,<<"setres">>,<<"set23">>,<<"set34">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"setres">>]),
	%% SINTERSTORE with non-set destination
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"foo">>]),
	5 = erldis_client:sr_scall(Client,[<<"sinterstore">>,<<"key1">>,<<"set1">>,<<"set2">>]),
	[<<"195">>,<<"196">>,<<"197">>,<<"198">>,<<"199">>] = lists:sort(erldis_client:scall(Client,[<<"smembers">>,<<"key1">>])),
	%% SINTERSTORE against non-set	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key2">>,<<"foo">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"sinterstore">>,<<"set1">>,<<"key2">>,<<"set2">>]),
	%% Bad Arguments
	{error,<<"ERR wrong number of arguments for 'SINTERSTORE' command">>}= erldis_client:sr_scall(Client,[<<"sinterstore">>]),
	{error,<<"ERR wrong number of arguments for 'SINTERSTORE' command">>}= erldis_client:sr_scall(Client,[<<"sinterstore">>,<<"set1">>]).

sunion(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	create_numeric_sets(Client),
	%% SUNION with two sets
	Expected = lists:sort(sets:to_list(sets:from_list(
											erldis_client:scall(Client,[<<"smembers">>,<<"set1">>]) ++
			     							erldis_client:scall(Client,[<<"smembers">>,<<"set2">>]) ))),
	Expected = lists:sort(erldis_client:scall(Client,[<<"sunion">>,<<"set1">>,<<"set2">>])),
	%% SUNION with non existing keys
	Expected = lists:sort(erldis_client:scall(Client,[<<"sunion">>,<<"nokey1">>,<<"set1">>,<<"set2">>,<<"nokey2">>])),
	%% SUNION against non-set
    ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"foo">>]),
    {error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"sunion">>,<<"key1">>,<<"set1">>]),
	%% Bad Arguments
	{error,<<"ERR wrong number of arguments for 'SUNION' command">>}= erldis_client:sr_scall(Client,[<<"sunion">>]).
