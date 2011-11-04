-module(lists_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
     [push_llen_lindex,del,long_list,
	  blpop_brpop,brpoplpush].
 	
init_per_testcase(_TestCase,Config) ->
	{ok,Client} = connect_erldis(10),

    erldis_client:sr_scall(Client,[<<"flushdb">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"I'm not a list">>]),

	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

connect_erldis(0) -> {error,{socket_error,econnrefused}};
connect_erldis(Times) ->
	timer:sleep(2000),
	case erldis:connect(localhost,16380) of
		{ok,Client} -> {ok,Client};
		_ -> connect_erldis(Times - 1)
	end.

-spec run_command(pid(),pid(),[binary()|integer()]) -> ok.   
run_command(Caller,Client, Args) ->
	Res = erldis_client:scall(Client,Args),
	Caller ! Res,
	ct:pal("~p~n",[Res]),
	ok.

push_llen_lindex(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	true = erldis_client:sr_scall(Client,[<<"lpush">>,<<"mylist">>,<<"a">>]),
	4 = erldis_client:sr_scall(Client,[<<"lpush">>,<<"mylist">>,<<"b">>,<<"c">>,<<"d">>]),
	
	5 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist">>,<<"e">>]),
	7 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist">>,<<"f">>,<<"g">>]),
	
	7 = erldis_client:sr_scall(Client,[<<"llen">>,<<"mylist">>]),
	false = erldis_client:sr_scall(Client,[<<"llen">>,<<"anotherlist">>]),
	
	<<"d">> = erldis_client:sr_scall(Client,[<<"lindex">>,<<"mylist">>,<<"0">>]),
	<<"a">> = erldis_client:sr_scall(Client,[<<"lindex">>,<<"mylist">>,<<"3">>]),
	<<"g">> = erldis_client:sr_scall(Client,[<<"lindex">>,<<"mylist">>,<<"-1">>]),
	<<"g">> = erldis_client:sr_scall(Client,[<<"lindex">>,<<"mylist">>,<<"6">>]),
	<<"e">> = erldis_client:sr_scall(Client,[<<"lindex">>,<<"mylist">>,<<"-3">>]),
	nil = erldis_client:sr_scall(Client,[<<"lindex">>,<<"mylist">>,<<"100">>]),
	nil = erldis_client:sr_scall(Client,[<<"lindex">>,<<"mylist">>,<<"-100">>]),
		   
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"lpush">>,<<"string">>,<<"a">>]),
	{error,<<"ERR wrong number of arguments for 'LPUSH' command">>} = erldis_client:sr_scall(Client,[<<"lpush">>,<<"mylist">>]),
	
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"rpush">>,<<"string">>,<<"a">>]),
	{error,<<"ERR wrong number of arguments for 'RPUSH' command">>} = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist">>]),
	
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"lindex">>,<<"string">>,<<"1">>]),
	{error,<<"ERR wrong number of arguments for 'LINDEX' command">>} = erldis_client:sr_scall(Client,[<<"lindex">>,<<"mylist">>]),
	{error,<<"ERR wrong number of arguments for 'LINDEX' command">>} = erldis_client:sr_scall(Client,[<<"lindex">>,<<"mylist">>,<<"4">>,<<"5">>]),
	
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"llen">>,<<"string">>]),	
	{error,<<"ERR wrong number of arguments for 'LLEN' command">>} = erldis_client:sr_scall(Client,[<<"llen">>,<<"mylist">>,<<"a">>]),
	{error,<<"ERR wrong number of arguments for 'LLEN' command">>} = erldis_client:sr_scall(Client,[<<"llen">>]).
	
del(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	true = erldis_client:sr_scall(Client,[<<"lpush">>,<<"mylist">>,<<"a">>]),
	true = erldis_client:sr_scall(Client,[<<"del">>,<<"mylist">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"mylist">>]).
	
%% Create a long list and check every single element with LINDEX
long_list(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	Elements = [edis_util:integer_to_binary(E)
			    || E <- lists:seq(0,999)], 
	
	%% Create Elements	
	[erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist">>,E])
     || E <- Elements],
    
    %% Check elements with positive and negative index
    [{E,E} = 
     {erldis:lindex(Client,<<"mylist">>,E),
      erldis:lindex(Client,<<"mylist">>,edis_util:integer_to_binary(-1000+edis_util:binary_to_integer(E)))}
    || E <- Elements].

blpop_brpop(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% Single existing list
	4 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist">>,<<"a">>,<<"b">>,<<"c">>,<<"d">>]),
	
	[<<"mylist">>,<<"a">>] = erldis_client:scall(Client,[<<"blpop">>,<<"mylist">>,1]),
	[<<"mylist">>,<<"d">>] = erldis_client:scall(Client,[<<"brpop">>,<<"mylist">>,1]),
	[<<"mylist">>,<<"b">>] = erldis_client:scall(Client,[<<"blpop">>,<<"mylist">>,1]),
	[<<"mylist">>,<<"c">>] = erldis_client:scall(Client,[<<"brpop">>,<<"mylist">>,1]),
    nil = erldis_client:sr_scall(Client,[<<"blpop">>,<<"mylist">>,1]),
	nil = erldis_client:sr_scall(Client,[<<"brpop">>,<<"mylist">>,1]),
	
	%% Multiple existing lists
	4 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist1">>,<<"a">>,<<"b">>,<<"c">>,<<"d">>]),
	4 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist2">>,<<"e">>,<<"f">>,<<"g">>,<<"h">>]),
	
	[<<"mylist1">>,<<"a">>] = erldis_client:scall(Client,[<<"blpop">>,<<"mylist1">>,<<"mylist2">>,1]),
	[<<"mylist1">>,<<"d">>] = erldis_client:scall(Client,[<<"brpop">>,<<"mylist1">>,<<"mylist2">>,1]),
	
	2 = erldis_client:sr_scall(Client,[<<"llen">>,<<"mylist1">>]),
	4 = erldis_client:sr_scall(Client,[<<"llen">>,<<"mylist2">>]),
	
	[<<"mylist2">>,<<"e">>] = erldis_client:scall(Client,[<<"blpop">>,<<"mylist2">>,<<"mylist1">>,1]),
	[<<"mylist2">>,<<"h">>] = erldis_client:scall(Client,[<<"brpop">>,<<"mylist2">>,<<"mylist1">>,1]),
	
	2 = erldis_client:sr_scall(Client,[<<"llen">>,<<"mylist1">>]),
	2 = erldis_client:sr_scall(Client,[<<"llen">>,<<"mylist2">>]),
	
	%% Second list has an entry
	4 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist4">>,<<"a">>,<<"b">>,<<"c">>,<<"d">>]),
	
	[<<"mylist4">>,<<"a">>] = erldis_client:scall(Client,[<<"blpop">>,<<"mylist3">>,<<"mylist4">>,1]),
	[<<"mylist4">>,<<"d">>] = erldis_client:scall(Client,[<<"brpop">>,<<"mylist3">>,<<"mylist4">>,1]),
	
	false = erldis_client:sr_scall(Client,[<<"llen">>,<<"mylist3">>]),
	2 = erldis_client:sr_scall(Client,[<<"llen">>,<<"mylist4">>]),
	
	{error,<<"ERR wrong number of arguments for 'BLPOP' command">>} = erldis_client:sr_scall(Client,[<<"blpop">>]),
	{error,<<"ERR wrong number of arguments for 'BLPOP' command">>} = erldis_client:sr_scall(Client,[<<"blpop">>,<<"mylist4">>]),
    {error,<<"ERR timeout is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"blpop">>,<<"mylist4">>,<<"mylist3">>]),
    {error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"blpop">>,<<"string">>,1]),
	
	{error,<<"ERR wrong number of arguments for 'BRPOP' command">>} = erldis_client:sr_scall(Client,[<<"brpop">>]),
	{error,<<"ERR wrong number of arguments for 'BRPOP' command">>} = erldis_client:sr_scall(Client,[<<"brpop">>,<<"mylist4">>]),
    {error,<<"ERR timeout is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"brpop">>,<<"mylist4">>,<<"mylist3">>]),
    {error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"brpop">>,<<"string">>,1]).

brpoplpush(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	{ok,Client2} = erldis:connect(localhost,16380),
	{ok,Client3} = erldis:connect(localhost,16380),
	
	%% Normal brpoplpush
	4 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist">>,<<"a">>,<<"b">>,<<"c">>,<<"d">>]),
	<<"d">> = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"mylist">>,<<"target">>,1]),
	<<"d">> = erldis_client:sr_scall(Client,[<<"rpop">>,<<"target">>]),
	[<<"a">>,<<"b">>,<<"c">>] = erldis_client:scall(Client,[<<"lrange">>,<<"mylist">>,0,-1]),
	
	%% With zero timeout should block indefinitely
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	
	spawn(?MODULE, run_command, [self(),Client2,[<<"brpoplpush">>,<<"mylist">>,<<"target">>,0]]),
	true = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist">>,<<"foo">>]),
	
	receive
		[<<"foo">>] -> ok;
		Resp -> ct:fail(Resp)
	after 2000 ->
		ct:fail({error, <<"ERR Timeout">>})
	end,

	[<<"foo">>] = erldis_client:scall(Client,[<<"lrange">>,<<"target">>,0,-1]),
	[] = erldis_client:scall(Client,[<<"lrange">>,<<"mylist">>,0,-1]),
	
	%% With a client BLPOPing the target list
    ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	
	spawn(erldis_client,scall,[Client2,[<<"blpop">>,<<"target">>,0]]),
	spawn(?MODULE, run_command, [self(),Client3,[<<"brpoplpush">>,<<"mylist">>,<<"target">>,0]]),
	true = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist">>,<<"foo">>]),
	
	receive
		[<<"foo">>] -> ok;
		Resp2 -> ct:fail(Resp2)
	after 2000 ->
		ct:fail("ERR Timeout")
	end,
	
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"target">>]),
	
	%% With wrong destination type
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"I'm not a list">>]),
	true = erldis_client:sr_scall(Client,[<<"rpush">>,<<"mylist">>,<<"foo">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"mylist">>,<<"string">>,1]),
	
	%% With wrong source type
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"I'm not a list">>]),
	true = erldis_client:sr_scall(Client,[<<"rpush">>,<<"target">>,<<"foo">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"string">>,<<"target">>,1]),
	
	%% Linked BRPOPLPUSH
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	
	spawn(erldis_client,scall,[Client,[<<"brpoplpush">>,<<"list1">>,<<"list2">>,0]]),
	spawn(erldis_client,scall,[Client2,[<<"brpoplpush">>,<<"list2">>,<<"list3">>,0]]),
	true = erldis_client:sr_scall(Client3,[<<"rpush">>,<<"list1">>,<<"foo">>]),
	
	nil = erldis_client:sr_scall(Client,[<<"lrange">>,<<"list1">>,0,-1]),
	nil = erldis_client:sr_scall(Client,[<<"lrange">>,<<"list2">>,0,-1]),
	<<"foo">> = erldis_client:sr_scall(Client,[<<"lrange">>,<<"list3">>,0,-1]),

    %% With multiple blocked clients
    ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
    ok = erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"I'm not a list">>]),
	
	spawn(?MODULE, run_command, [self(),Client,[<<"brpoplpush">>,<<"mylist">>,<<"string">>,0]]),
	spawn(?MODULE, run_command, [self(),Client2,[<<"brpoplpush">>,<<"mylist">>,<<"target">>,0]]),
	true = erldis_client:sr_scall(Client3,[<<"rpush">>,<<"mylist">>,<<"testtest">>]),

	receive
		[{error,<<"ERR Operation against a key holding the wrong kind of value">>}] -> ok;
		[<<"testtest">>] -> ok
	after 2000 ->
		ct:fail("ERR Timeout")
	end,

	receive
		[{error,<<"ERR Operation against a key holding the wrong kind of value">>}] -> ok;
		[<<"testtest">>] -> ok
	after 2000 ->
		ct:fail("ERR Timeout")
	end,
	
	<<"testtest">> = erldis_client:sr_scall(Client,[<<"lrange">>,<<"target">>,0,-1]),
	
	%% Circular
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	
	spawn(erldis_client,scall,[Client,[<<"brpoplpush">>,<<"list1">>,<<"list2">>,0]]),
	spawn(erldis_client,scall,[Client2,[<<"brpoplpush">>,<<"list2">>,<<"list1">>,0]]),
	true = erldis_client:sr_scall(Client3,[<<"rpush">>,<<"list1">>,<<"foo">>]),
	timer:sleep(1000),
	
	<<"foo">> = erldis_client:sr_scall(Client3,[<<"lrange">>,<<"list1">>,0,-1]),
	nil = erldis_client:sr_scall(Client3,[<<"lrange">>,<<"list2">>,0,-1]),
	
	%% Self-referential BRPOPLPUSH
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	spawn(erldis_client,scall,[Client,[<<"brpoplpush">>,<<"list">>,<<"list">>,0]]),
	true = erldis_client:sr_scall(Client3,[<<"rpush">>,<<"list">>,<<"foo">>]),
	
	<<"foo">> = erldis_client:sr_scall(Client3,[<<"lrange">>,<<"list">>,0,-1]),
	
	%% Inside a transaction
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	true = erldis_client:sr_scall(Client,[<<"lpush">>,<<"xlist">>,<<"foo">>]),
	2 = erldis_client:sr_scall(Client,[<<"lpush">>,<<"xlist">>,<<"bar">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"multi">>]),
	queued = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"xlist">>,<<"target">>,1]),
	queued = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"xlist">>,<<"target">>,1]),
	queued = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"xlist">>,<<"target">>,1]),
	queued = erldis_client:scall(Client,[<<"lrange">>,<<"xlist">>,0,-1]),
	queued = erldis_client:scall(Client,[<<"lrange">>,<<"target">>,0,-1]),

	[<<"foo">>,<<"bar">>,nil,<<"*0">>,<<"*2">>] = erldis_client:scall(Client,[<<"exec">>]),
	
	%% Timeout
	[] = erldis_client:scall(Client,[<<"brpoplpush">>,<<"list1">>,<<"list2">>,1]),
	
	{error,<<"ERR wrong number of arguments for 'BRPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"l1">>,<<"l2">>]),
	{error,<<"ERR wrong number of arguments for 'BRPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"l1">>]),
	{error,<<"ERR wrong number of arguments for 'BRPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>]),
	{error,<<"ERR wrong number of arguments for 'BRPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"l1">>,<<"l2">>,1,1]),
	{error,<<"ERR timeout is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"l1">>,<<"l2">>,<<"str">>]).