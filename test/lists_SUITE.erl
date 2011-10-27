-module(lists_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
 	[push_llen_lindex,del,long_list].
 	
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