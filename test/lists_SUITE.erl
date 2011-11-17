-module(lists_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
     [push_llen_lindex,del,long_list,
	  blpop_brpop,brpoplpush,lpushx_rpushx,
	  linsert,rpoplpush,lpop_rpop,lrange].
 	
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
	nil = erldis_client:sr_scall(Client,[<<"lindex">>,<<"anotherlist">>,1]),
		   
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
	
	%% Extra tests
	ok = bpop(<<"blpop">>,Client),
	ok = bpop(<<"brpop">>,Client),

	{error,<<"ERR wrong number of arguments for 'BLPOP' command">>} = erldis_client:sr_scall(Client,[<<"blpop">>]),
	{error,<<"ERR wrong number of arguments for 'BLPOP' command">>} = erldis_client:sr_scall(Client,[<<"blpop">>,<<"mylist4">>]),
    {error,<<"ERR timeout is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"blpop">>,<<"mylist4">>,<<"mylist3">>]),
    {error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"blpop">>,<<"string">>,1]),
	
	{error,<<"ERR wrong number of arguments for 'BRPOP' command">>} = erldis_client:sr_scall(Client,[<<"brpop">>]),
	{error,<<"ERR wrong number of arguments for 'BRPOP' command">>} = erldis_client:sr_scall(Client,[<<"brpop">>,<<"mylist4">>]),
    {error,<<"ERR timeout is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"brpop">>,<<"mylist4">>,<<"mylist3">>]),
    {error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"brpop">>,<<"string">>,1]).

bpop(Command,Client) ->	
	{ok,Client2} = erldis:connect(localhost,16380),
	%% With single empty list argument
	spawn(?MODULE, run_command, [self(),Client2,[Command,<<"list">>,2]]),
	true = erldis_client:sr_scall(Client,[<<"lpush">>,<<"list">>,<<"foo">>]),
	receive 
		[<<"list">>,<<"foo">>] -> ok;
		Resp -> ct:fail(Resp) 
	after 4000 -> ct:fail("ERR Timeout")
	end,
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"list">>]),
	
	%% With negative timeout
	{error,<<"ERR timeout is negative">>} = erldis_client:sr_scall(Client,[Command,<<"list">>,-1]),
	
	%% With non-integer timeout
	{error,<<"ERR timeout is not an integer or out of range">>} = erldis_client:sr_scall(Client,[Command,<<"list">>,2.3]),
	
	%% With zero timeout should block indefinitely
	spawn(?MODULE, run_command, [self(),Client2,[Command,<<"list">>,0]]),
	timer:sleep(1000),
	erldis_client:sr_scall(Client,[<<"rpush">>,<<"list">>,<<"bar">>]),
	receive 
		[<<"list">>,<<"bar">>] -> ok;
		Resp2 -> ct:fail(Resp2) 
	after 4000 -> ct:fail("ERR Timeout")
	end,
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"list">>]),
	
	%% Timeout
	nil = erldis_client:sr_scall(Client,[Command,<<"list">>,<<"list2">>,1]),
	
	%% Arguments are empty
	spawn(?MODULE, run_command, [self(),Client2,[Command,<<"list4">>,<<"list5">>,1]]),
	erldis_client:sr_scall(Client,[<<"rpush">>,<<"list4">>,<<"bar">>]),
	receive 
		[<<"list4">>,<<"bar">>] -> ok;
		Resp3 -> ct:fail(Resp3) 
	after 4000 -> ct:fail("ERR Timeout")
	end,
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"list4">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"list5">>]),
	
	spawn(?MODULE, run_command, [self(),Client2,[Command,<<"list4">>,<<"list5">>,1]]),
	erldis_client:sr_scall(Client,[<<"rpush">>,<<"list5">>,<<"buzz">>]),
	receive 
		[<<"list5">>,<<"buzz">>] -> ok;
		Resp4 -> ct:fail(Resp4) 
	after 4000 -> ct:fail("ERR Timeout")
	end,
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"list4">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"list5">>]),
	
	ok.

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
	after 4000 ->
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
	after 4000 ->
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
	after 4000 ->
		ct:fail("ERR Timeout")
	end,

	receive
		[{error,<<"ERR Operation against a key holding the wrong kind of value">>}] -> ok;
		[<<"testtest">>] -> ok
	after 6000 ->
		ct:fail("ERR Timeout")
	end,
	
	<<"testtest">> = erldis_client:sr_scall(Client,[<<"lrange">>,<<"target">>,0,-1]),
	
	%% Circular
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	
	spawn(erldis_client,scall,[Client,[<<"brpoplpush">>,<<"list1">>,<<"list2">>,0]]),
	spawn(erldis_client,scall,[Client2,[<<"brpoplpush">>,<<"list2">>,<<"list1">>,0]]),
	true = erldis_client:sr_scall(Client3,[<<"rpush">>,<<"list1">>,<<"foobar">>]),
	timer:sleep(1000),
	
	<<"foobar">> = erldis_client:sr_scall(Client3,[<<"lrange">>,<<"list1">>,0,-1]),
	nil = erldis_client:sr_scall(Client3,[<<"lrange">>,<<"list2">>,0,-1]),
	
	%% Self-referential BRPOPLPUSH
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	spawn(erldis_client,scall,[Client,[<<"brpoplpush">>,<<"list">>,<<"list">>,0]]),
	true = erldis_client:sr_scall(Client3,[<<"rpush">>,<<"list">>,<<"foo">>]),
	
	<<"foo">> = erldis_client:sr_scall(Client3,[<<"lrange">>,<<"list">>,0,-1]),
	
	%% Inside a transaction
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	true = erldis_client:sr_scall(Client,[<<"lpush">>,<<"xxlist">>,<<"foo">>]),
	2 = erldis_client:sr_scall(Client,[<<"lpush">>,<<"xxlist">>,<<"bar">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"multi">>]),
	queued = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"xxlist">>,<<"target">>,1]),
	queued = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"xxlist">>,<<"target">>,1]),
	queued = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"xxlist">>,<<"target">>,1]),
	queued = erldis_client:sr_scall(Client,[<<"lrange">>,<<"xxlist">>,0,-1]),
	queued = erldis_client:sr_scall(Client,[<<"llen">>,<<"target">>]),	
	
	[<<"foo">>,<<"bar">>,nil,<<"*0">>,2] = erldis_client:scall(Client,[<<"exec">>]),
	
	%% Timeout
	[] = erldis_client:scall(Client,[<<"brpoplpush">>,<<"list1">>,<<"list2">>,1]),
	
	{error,<<"ERR wrong number of arguments for 'BRPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"l1">>,<<"l2">>]),
	{error,<<"ERR wrong number of arguments for 'BRPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"l1">>]),
	{error,<<"ERR wrong number of arguments for 'BRPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>]),
	{error,<<"ERR wrong number of arguments for 'BRPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"l1">>,<<"l2">>,1,1]),
	{error,<<"ERR timeout is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"brpoplpush">>,<<"l1">>,<<"l2">>,<<"str">>]).

lpushx_rpushx(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% Empty List
	false = erldis_client:sr_scall(Client,[<<"lpushx">>,<<"xlist">>,<<"a">>]),
	false = erldis_client:sr_scall(Client,[<<"llen">>,<<"xlist">>]),
	false = erldis_client:sr_scall(Client,[<<"rpushx">>,<<"xlist">>,<<"a">>]),
	false = erldis_client:sr_scall(Client,[<<"llen">>,<<"xlist">>]),
	
	%% Existing list
	true = erldis_client:sr_scall(Client,[<<"rpush">>,<<"xlist">>,<<"b">>]),
	2 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"xlist">>,<<"c">>]),
	
	3 = erldis_client:sr_scall(Client,[<<"lpushx">>,<<"xlist">>,<<"a">>]),
	4 = erldis_client:sr_scall(Client,[<<"rpushx">>,<<"xlist">>,<<"d">>]),
	[<<"a">>,<<"b">>,<<"c">>,<<"d">>] = erldis_client:scall(Client,[<<"lrange">>,<<"xlist">>,0,-1]),
	
	{error,<<"ERR wrong number of arguments for 'LPUSHX' command">>} = erldis_client:sr_scall(Client,[<<"lpushx">>,<<"xlist">>]),
	{error,<<"ERR wrong number of arguments for 'LPUSHX' command">>} = erldis_client:sr_scall(Client,[<<"lpushx">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"lpushx">>,<<"string">>,<<"foo">>]),
	{error,<<"ERR wrong number of arguments for 'RPUSHX' command">>} = erldis_client:sr_scall(Client,[<<"rpushx">>,<<"xlist">>]),
	{error,<<"ERR wrong number of arguments for 'RPUSHX' command">>} = erldis_client:sr_scall(Client,[<<"rpushx">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"rpushx">>,<<"string">>,<<"foo">>]).

linsert(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% Simple LINSERT
	3 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"xlist">>,<<"a">>,<<"foo">>,<<"bar">>]),
	4 = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"before">>,<<"foo">>,<<"zz">>]),
	[<<"a">>,<<"zz">>,<<"foo">>,<<"bar">>] = erldis_client:scall(Client,[<<"lrange">>,<<"xlist">>,0,-1]),
	5 = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"after">>,<<"a">>,<<"yy">>]),
	[<<"a">>,<<"yy">>,<<"zz">>,<<"foo">>,<<"bar">>] = erldis_client:scall(Client,[<<"lrange">>,<<"xlist">>,0,-1]),
	-1 = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"after">>,<<"bad">>,<<"yy">>]),
	[<<"a">>,<<"yy">>,<<"zz">>,<<"foo">>,<<"bar">>] = erldis_client:scall(Client,[<<"lrange">>,<<"xlist">>,0,-1]),
	6 = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"after">>,<<"yy">>,<<"xx">>]),
	-1 = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"before">>,<<"bad">>,<<"yy">>]),
	
	{error,<<"ERR wrong number of arguments for 'LINSERT' command">>} = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"after">>,<<"a">>,<<"b">>,<<"c">>]),
	{error,<<"ERR wrong number of arguments for 'LINSERT' command">>} = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"after">>,<<"a">>]),
	{error,<<"ERR wrong number of arguments for 'LINSERT' command">>} = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"after">>]),
	{error,<<"ERR wrong number of arguments for 'LINSERT' command">>} = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"after">>]),
	{error,<<"ERR wrong number of arguments for 'LINSERT' command">>} = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>]),
	{error,<<"ERR wrong number of arguments for 'LINSERT' command">>} = erldis_client:sr_scall(Client,[<<"linsert">>]),
	{error,<<"ERR syntax error">>} = erldis_client:sr_scall(Client,[<<"linsert">>,<<"xlist">>,<<"wherever">>,<<"a">>,<<"b">>]).

rpoplpush(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% Base Case
	3 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"list1">>,<<"a">>,<<"foo">>,<<"bar">>]),
	<<"bar">> = erldis_client:sr_scall(Client,[<<"rpoplpush">>,<<"list1">>,<<"list2">>]),
	<<"foo">> = erldis_client:sr_scall(Client,[<<"rpoplpush">>,<<"list1">>,<<"list2">>]),
	[<<"a">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list1">>,0,-1]),
	[<<"foo">>,<<"bar">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list2">>,0,-1]),
	
	%% With the same list as src and dst
	4 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"list3">>,<<"a">>,<<"b">>,<<"c">>,<<"d">>]),
	[<<"a">>,<<"b">>,<<"c">>,<<"d">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list3">>,0,-1]),
	<<"d">> = erldis_client:sr_scall(Client,[<<"rpoplpush">>,<<"list3">>,<<"list3">>]),
	[<<"d">>,<<"a">>,<<"b">>,<<"c">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list3">>,0,-1]),
	
	%% Non existing key
	nil = erldis_client:sr_scall(Client,[<<"rpoplpush">>,<<"list4">>,<<"list5">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"list4">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"list5">>]),
	
	%% Non list src key
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"rpoplpush">>,<<"string">>,<<"list5">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"list5">>]),
	
	%% Non list dest key
	3 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"list5">>,<<"a">>,<<"b">>,<<"c">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"rpoplpush">>,<<"list5">>,<<"string">>]),
	[<<"a">>,<<"b">>,<<"c">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list5">>,0,-1]),
	
	{error,<<"ERR wrong number of arguments for 'RPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"rpoplpush">>,<<"list5">>]),
	{error,<<"ERR wrong number of arguments for 'RPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"rpoplpush">>]),
	{error,<<"ERR wrong number of arguments for 'RPOPLPUSH' command">>} = erldis_client:sr_scall(Client,[<<"rpoplpush">>,<<"list5">>,<<"list1">>,<<"list2">>]).

lpop_rpop(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% Basic
	3 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"list1">>,<<"a">>,<<"b">>,<<"c">>]),
	<<"a">> = erldis_client:sr_scall(Client,[<<"lpop">>,<<"list1">>]),
	<<"c">> = erldis_client:sr_scall(Client,[<<"rpop">>,<<"list1">>]),
	<<"b">> = erldis_client:sr_scall(Client,[<<"lpop">>,<<"list1">>]),
	nil = erldis_client:sr_scall(Client,[<<"rpop">>,<<"list1">>]),
	nil = erldis_client:sr_scall(Client,[<<"lpop">>,<<"list1">>]),
	
	%% Massive
	Elements = [edis_util:integer_to_binary(E)
			    || E <- lists:seq(0,999)], 
	[erldis_client:sr_scall(Client,[<<"rpush">>,<<"list2">>,E])
     || E <- Elements],
	PopResults = [{edis_util:integer_to_binary(E),edis_util:integer_to_binary(999-E)}|| E <- lists:seq(0,499)],
	[{First,Last} = 
     {erldis_client:sr_scall(Client,[<<"lpop">>,<<"list2">>]),
	  erldis_client:sr_scall(Client,[<<"rpop">>,<<"list2">>])}
    || {Fist,Last} <- PopResults],
	[] = erldis_client:scall(Client,[<<"lrange">>,<<"list2">>,0,-1]),
	
	%% Non list value
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"rpop">>,<<"string">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"lpop">>,<<"string">>]),
	{error,<<"ERR wrong number of arguments for 'RPOP' command">>} = erldis_client:sr_scall(Client,[<<"rpop">>]),
	{error,<<"ERR wrong number of arguments for 'LPOP' command">>} = erldis_client:sr_scall(Client,[<<"lpop">>,<<"list1">>,<<"list2">>]).

lrange(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% Basic
	8 = erldis_client:sr_scall(Client,[<<"rpush">>,<<"list1">>,0,1,2,3,4,5,6,7]),
	[<<"1">>,<<"2">>,<<"3">>,<<"4">>,<<"5">>,<<"6">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list1">>,1,-2]),
	[<<"5">>,<<"6">>,<<"7">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list1">>,-3,-1]),
	[<<"4">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list1">>,4,4]),
	%% Inverted indexes
	[] = erldis_client:scall(Client,[<<"lrange">>,<<"list1">>,6,4]),
	%% Out of range indexes including the full list
	[<<"0">>,<<"1">>,<<"2">>,<<"3">>,<<"4">>,<<"5">>,<<"6">>,<<"7">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list1">>,-1000,1000]),
	%% Out of range negative end index
	[<<"0">>] = erldis_client:scall(Client,[<<"lrange">>,<<"list1">>,0,-8]),
	[] = erldis_client:scall(Client,[<<"lrange">>,<<"list1">>,0,-9]),
	%% Non existing key
	[] = erldis_client:scall(Client,[<<"lrange">>,<<"list2">>,0,1]),
	
	[{error,<<"ERR Operation against a key holding the wrong kind of value">>}] = erldis_client:scall(Client,[<<"lrange">>,<<"string">>,0,1]),
	{error,<<"ERR wrong number of arguments for 'LRANGE' command">>} = erldis_client:scall(Client,[<<"lrange">>,<<"list2">>,0]),
	{error,<<"ERR wrong number of arguments for 'LRANGE' command">>} = erldis_client:scall(Client,[<<"lrange">>,<<"list2">>]),
	{error,<<"ERR wrong number of arguments for 'LRANGE' command">>} = erldis_client:scall(Client,[<<"lrange">>,<<"list2">>,0,1,3]).