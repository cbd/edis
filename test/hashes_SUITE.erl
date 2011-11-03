-module(hashes_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
	[hset_hlen,hset_hget,hsetnx,hmset,hkeys,hvals,
	 hgetall,hdel,hexists,hincrby].

init_per_testcase(_TestCase,Config) ->
	{ok,Client} = connect_erldis(10),
    ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	
	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

connect_erldis(0) -> {error,{socket_error,econnrefused}};
connect_erldis(Times) ->
	timer:sleep(2000),
	case erldis:connect(localhost,16380) of
		{ok,Client} -> {ok,Client};
		_ -> connect_erldis(Times - 1)
	end.

hset_hlen(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
		
	%% Small hash creation
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"smallhash">>,edis_util:random_binary(),edis_util:random_binary()])
	|| _ <- lists:seq(1,8)],
	8 = erldis_client:sr_scall(Client,[<<"hlen">>,<<"smallhash">>]),
	
	%% Big hash creation
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"bighash">>,edis_util:random_binary(),edis_util:random_binary()])
	|| _ <- lists:seq(1,1024)],
	1024 = erldis_client:sr_scall(Client,[<<"hlen">>,<<"bighash">>]),
	
	{error,<<"ERR wrong number of arguments for 'HSET' command">>} = erldis_client:sr_scall(Client,[<<"hset">>,<<"smallhash">>,<<"field">>]),
	{error,<<"ERR wrong number of arguments for 'HSET' command">>} = erldis_client:sr_scall(Client,[<<"hset">>,<<"smallhash">>,<<"field">>,<<"value">>,<<"field2">>]),
	{error,<<"ERR wrong number of arguments for 'HLEN' command">>} = erldis_client:sr_scall(Client,[<<"hlen">>,<<"smallhash">>,<<"field">>]),
	{error,<<"ERR wrong number of arguments for 'HLEN' command">>} = erldis_client:sr_scall(Client,[<<"hlen">>]).

hset_hget(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,edis_util:integer_to_binary(E),list_to_binary("value "++integer_to_list(E))])
	|| E <- lists:seq(1,8)],
	
	<<"value 1">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"1">>]),
	<<"value 5">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"5">>]),
	<<"value 8">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"8">>]),
	nil = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"9">>]),
	nil = erldis_client:sr_scall(Client,[<<"hget">>,<<"nonexistinghash">>,<<"3">>]),
	
	%% Set Already existing field
	false = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,<<"5">>,<<"foo">>]),
	<<"foo">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"5">>]),

	%% Add new field
	true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,<<"9">>,<<"bar">>]),
	<<"bar">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"9">>]),
    
	%% Long keys
	LongKey = list_to_binary(["k" || _ <- lists:seq(1, 500)]),
	erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,LongKey,<<"a">>]),
	erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,LongKey,<<"b">>]),
	<<"b">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,LongKey]),
	
	{error,<<"ERR wrong number of arguments for 'HGET' command">>} = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>]),
	{error,<<"ERR wrong number of arguments for 'HGET' command">>} = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"8">>,<<"9">>]).

hsetnx(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% With not existing key
	true = erldis_client:sr_scall(Client,[<<"hsetnx">>,<<"myhash">>,<<"field">>,<<"foo">>]),
	<<"foo">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"field">>]),
	
	%% With not existing field
	true = erldis_client:sr_scall(Client,[<<"hsetnx">>,<<"myhash">>,<<"field2">>,<<"bar">>]),
	<<"bar">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"field2">>]),
	
	%% With already existing field
	false = erldis_client:sr_scall(Client,[<<"hsetnx">>,<<"myhash">>,<<"field">>,<<"buzz">>]),
	<<"foo">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"field">>]),
	
	{error,<<"ERR wrong number of arguments for 'HSETNX' command">>} = erldis_client:sr_scall(Client,[<<"hsetnx">>,<<"myhash">>,<<"field">>,<<"foo">>,<<"buzz">>]),
	{error,<<"ERR wrong number of arguments for 'HSETNX' command">>} = erldis_client:sr_scall(Client,[<<"hsetnx">>,<<"myhash">>,<<"field">>]),
	{error,<<"ERR wrong number of arguments for 'HSETNX' command">>} = erldis_client:sr_scall(Client,[<<"hsetnx">>,<<"myhash">>]),
	{error,<<"ERR wrong number of arguments for 'HSETNX' command">>} = erldis_client:sr_scall(Client,[<<"hsetnx">>]).

hmset(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% With not existing key
	ok = erldis_client:sr_scall(Client,[<<"hmset">>,<<"myhash">>,<<"field">>,<<"foo">>,<<"field2">>,<<"bar">>,<<"field3">>,<<"buzz">>]),
	<<"bar">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"field2">>]),
	
	%% With not existing field
	ok = erldis_client:sr_scall(Client,[<<"hmset">>,<<"myhash">>,<<"field4">>,<<"hello">>,<<"field5">>,<<"world">>]),
	<<"hello">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"field4">>]),
	
	%% With already existing field
	ok = erldis_client:sr_scall(Client,[<<"hmset">>,<<"myhash">>,<<"field4">>,<<"hi">>,<<"field5">>,<<"edis">>]),
	<<"edis">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"field5">>]),

    {error,<<"ERR wrong number of arguments for HMSET">>} = erldis_client:sr_scall(Client,[<<"hmset">>,<<"myhash">>,<<"field">>,<<"foo">>,<<"field2">>]),
    {error,<<"ERR wrong number of arguments for HMSET">>} = erldis_client:sr_scall(Client,[<<"hmset">>,<<"myhash">>,<<"field">>]),
    {error,<<"ERR wrong number of arguments for HMSET">>} = erldis_client:sr_scall(Client,[<<"hmset">>,<<"myhash">>]),
    {error,<<"ERR wrong number of arguments for HMSET">>} = erldis_client:sr_scall(Client,[<<"hmset">>]).

hmget(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,edis_util:integer_to_binary(E),list_to_binary("value "++integer_to_list(E))])
	|| E <- lists:seq(1,8)],
	
	[<<"value 1">>,<<"value 2">>,<<"value 5">>,<<"value 8">>] = erldis_client:scall(Client,[<<"hmget">>,<<"myhash">>,<<"1">>,<<"2">>,<<"5">>,<<"8">>]),
	
	%% Not existing hash
	[nil,nil,nil,nil] = erldis_client:scall(Client,[<<"hmget">>,<<"notexisting">>,<<"1">>,<<"2">>,<<"5">>,<<"8">>]),
	
	%% Not existing field
	[<<"value 1">>,nil,nil,<<"value 11">>] = erldis_client:scall(Client,[<<"hmget">>,<<"myhash">>,<<"1">>,<<"9">>,<<"11">>,<<"8">>]),
	
	{error,<<"ERR wrong number of arguments for 'HMGET' command">>} = erldis_client:scall(Client,[<<"hmget">>,<<"myhash">>]),
	{error,<<"ERR wrong number of arguments for 'HMGET' command">>} = erldis_client:scall(Client,[<<"hmget">>]).
	
hkeys(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,edis_util:integer_to_binary(E),list_to_binary("value "++integer_to_list(E))])
	|| E <- lists:seq(1,8)],
	
	[<<"1">>,<<"2">>,<<"3">>,<<"4">>,<<"5">>,<<"6">>,<<"7">>,<<"8">>] = lists:sort(erldis_client:scall(Client,[<<"hkeys">>,<<"myhash">>])),
	
	%% Not existing hash
	[] = erldis_client:scall(Client,[<<"hkeys">>,<<"notexisting">>]),
	
	[{error,<<"ERR wrong number of arguments for 'HKEYS' command">>}] = erldis_client:scall(Client,[<<"hkeys">>]),
	[{error,<<"ERR wrong number of arguments for 'HKEYS' command">>}] = erldis_client:scall(Client,[<<"hkeys">>,<<"myhash">>,<<"1">>]).

hvals(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,list_to_binary("key "++integer_to_list(E)),edis_util:integer_to_binary(E)])
	|| E <- lists:seq(1,8)],
	
	[<<"1">>,<<"2">>,<<"3">>,<<"4">>,<<"5">>,<<"6">>,<<"7">>,<<"8">>] = lists:sort(erldis_client:scall(Client,[<<"hvals">>,<<"myhash">>])),
	
	%% Not existing hash
	[] = erldis_client:scall(Client,[<<"hvals">>,<<"notexisting">>]),
	
	[{error,<<"ERR wrong number of arguments for 'HVALS' command">>}] = erldis_client:scall(Client,[<<"hvals">>]),
	[{error,<<"ERR wrong number of arguments for 'HVALS' command">>}] = erldis_client:scall(Client,[<<"hvals">>,<<"myhash">>,<<"1">>]).
	
hgetall(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	Hash = lists:sort([{list_to_binary("key "++integer_to_list(E)),list_to_binary("value "++integer_to_list(E))}
			|| E <- lists:seq(1,8)]),
	
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,Key,Value])
	|| {Key,Value} <- Hash],
	
	Hash = lists:sort(edis_util:make_pairs(erldis_client:scall(Client,[<<"hgetall">>,<<"myhash">>]))),
	
	[{error,<<"ERR wrong number of arguments for 'HGETALL' command">>}] = erldis_client:scall(Client,[<<"hgetall">>,<<"myhash">>,<<"key 1">>]),
	[{error,<<"ERR wrong number of arguments for 'HGETALL' command">>}] = erldis_client:scall(Client,[<<"hgetall">>]).
	
hdel(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,list_to_binary("key "++integer_to_list(E)),edis_util:integer_to_binary(E)])
	|| E <- lists:seq(1,8)],
	
	true = erldis_client:sr_scall(Client,[<<"hdel">>,<<"myhash">>,<<"key 2">>]),
	nil = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"key 2">>]),
	
	3 = erldis_client:sr_scall(Client,[<<"hdel">>,<<"myhash">>,<<"key 4">>,<<"key 1">>,<<"key 7">>]),
	nil = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"key 1">>]),
	nil = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"key 4">>]),
	nil = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"key 7">>]),
		
	false = erldis_client:sr_scall(Client,[<<"hdel">>,<<"myhash">>,<<"key 34">>]),
	false = erldis_client:sr_scall(Client,[<<"hdel">>,<<"notexisting">>,<<"key 6">>]),
	
	{error,<<"ERR wrong number of arguments for 'HDEL' command">>} = erldis_client:sr_scall(Client,[<<"hdel">>,<<"myhash">>]),
	{error,<<"ERR wrong number of arguments for 'HDEL' command">>} = erldis_client:sr_scall(Client,[<<"hdel">>]).
	
hexists(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,list_to_binary("key "++integer_to_list(E)),edis_util:integer_to_binary(E)])
	|| E <- lists:seq(1,8)],
	
	true = erldis_client:sr_scall(Client,[<<"hexists">>,<<"myhash">>,<<"key 5">>]),
	false = erldis_client:sr_scall(Client,[<<"hexists">>,<<"myhash">>,<<"key 10">>]),
	
	{error,<<"ERR wrong number of arguments for 'HEXISTS' command">>} = erldis_client:sr_scall(Client,[<<"hexists">>,<<"myhash">>]),
	{error,<<"ERR wrong number of arguments for 'HEXISTS' command">>} = erldis_client:sr_scall(Client,[<<"hexists">>,<<"myhash">>,<<"key 1">>,<<"key 14">>]).

hincrby(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	4 = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 1">>,<<"4">>]),
	7 = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 1">>,<<"3">>]),
	true = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 1">>,<<"-6">>]),
	-14 = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 1">>,<<"-15">>]),
		
	false = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,<<"key 1">>,<<"17179869184">>]),
	17179869185 = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 1">>,<<"1">>]),
	19179969185 = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 1">>,<<"2000100000">>]),
	19179969085 = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 1">>,<<"-100">>]),
	-20000 = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 1">>,<<"-19179989085">>]),
	
	true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,<<"key 2">>,<<"4ed3">>]),
    {error,<<"ERR hash value is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 2">>,<<"120">>]),
	
	true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,<<"key 3">>,<<"1236">>]),
	{error,<<"ERR value is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 3">>,<<"1a6">>]),
	
	{error,<<"ERR wrong number of arguments for 'HINCRBY' command">>} = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 3">>,<<"16">>,<<"435">>]),
	{error,<<"ERR wrong number of arguments for 'HINCRBY' command">>} = erldis_client:sr_scall(Client,[<<"hincrby">>,<<"myhash">>,<<"key 3">>]).
