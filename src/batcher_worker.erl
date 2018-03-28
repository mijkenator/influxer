-module(batcher_worker).

-behaviour(gen_server).

-export([ 
         start_link/1,
         batch_write/2,
         write/2,
         change_batch_params/2,
         test/0
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(BATCH_TIMEOUT, 5000).
-define(BATCH_MAX_COUNT, 5000).

-record(state, {time = 0, count = 0, buf = <<>>, 
                auth_token = undefined, burl = undefined,
                timeout = 2000, batch_timeout = ?BATCH_TIMEOUT, 
                batch_max_count = ?BATCH_MAX_COUNT}).

%
% influxer_app:start().
% batcher_sup:start_child(b1, [<<"http://10.0.0.85:8086/write?db=mkh_data">>, <<"root">>,<<"Rbccm2018">>,1,2000,1000]).
% batcher_worker:write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}], [{<<"value">>, 1.5},{<<"average">>, 2.5},{<<"val2">>, 3.5}]}).
%

test() ->
    influxer_app:start(),
    batcher_sup:start_child(b1, [<<"http://10.0.0.85:8086/write?db=mkh_data">>, <<"root">>,<<"Rbccm2018">>,1,2000,1000]),
    lager:set_loglevel(lager_console_backend, debug),
    batcher_worker:batch_write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}], [{<<"value">>, 2.25},{<<"average">>, 2.25},{<<"val2">>, 3.25}]}),
    lager:debug("1", []),
    batcher_worker:batch_write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}], [{<<"value">>, 3.26},{<<"average">>, 2.26},{<<"val2">>, 3.26}]}),
    lager:debug("2", []),
    batcher_worker:batch_write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}], [{<<"value">>, 4.27},{<<"average">>, 2.27},{<<"val2">>, 3.27}]}),
    lager:debug("3", []),
    batcher_worker:batch_write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}], [{<<"value">>, 5.28},{<<"average">>, 2.28},{<<"val2">>, 3.28}]}),
    lager:debug("4", []),
    batcher_worker:batch_write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}], [{<<"value">>, 6.29},{<<"average">>, 2.29},{<<"val2">>, 3.29}]}),
    lager:debug("5", []),
    batcher_worker:write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}], [{<<"value">>, 7.29},{<<"average">>, 2.29},{<<"val2">>, 3.29}]}),
    ok.

change_batch_params(Server, Params) ->
    gen_server:cast(Server, {change_batch_params, Params}).

batch_write(Server, {Table, Tags, Values}) ->
    gen_server:cast(Server, {batch_write, Table, Tags, Values}).

write(Server, {Table, Tags, Values}) ->
    gen_server:cast(Server, {write, Table, Tags, Values}).

start_link(Name) when is_atom(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []);
start_link([Name | Args]) ->
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

init([Url, Login, Password, PoolSize, TimeOut, BatchMaxSize]) ->
    lager:debug("Batcher worker with pool start ~p ~n", [{Url, Login, Password, PoolSize, TimeOut, BatchMaxSize}]),
    influxer_utils:start_pool({Url, [{pool_size, PoolSize}]}),
    BUrl = buoy_utils:parse_url(Url),
    AuthToken = influxer_utils:get_auth_token(Login, Password),
    timer:send_after(?BATCH_TIMEOUT, self(), batch_out),
    {ok, #state{auth_token = AuthToken, burl = BUrl, timeout = TimeOut, 
                batch_max_count = BatchMaxSize}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({change_batch_params, Params}, #state{batch_timeout = BT, batch_max_count = BC} = State) ->
    NBT = proplists:get_value(batch_timeout,   Params, BT),
    NBC = proplists:get_value(batch_max_count, Params, BC),
    {noreply, State#state{batch_max_count = NBC, batch_timeout = NBT}};
handle_cast({batch_write, Table, Tags, Values}, 
            #state{time=T, count=C, buf=B, 
                   batch_max_count = BMC, burl = BUrl, 
                   auth_token = AuthToken, timeout = TimeOut} = State) ->
    TagsBin   = lists:foldl(fun influxer_utils:tag_to_bin/2, <<>>, Tags),
    ValuesBin = lists:foldl(fun influxer_utils:tag_to_bin/2, <<>>, Values),
    TStamp    = integer_to_binary(erlang:system_time(nanosecond)),
    Body      = <<Table/binary,",", TagsBin/binary, " ", ValuesBin/binary, " ", TStamp/binary, "\n">>, 
    NB        = <<B/binary, Body/binary>>,
    CT = erlang:system_time(millisecond),
    T1 = case T of 0 -> CT; _ -> T end,
    case {C > BMC, CT - T1 > ?BATCH_TIMEOUT} of
        {false, false} -> 
            lager:debug("Influxer batch write save buf: ~p", [NB]),
            {noreply, State#state{count=C+1, buf=NB}}
        ;_ ->
            Headers   = [{<<"Authorization">>, AuthToken},
                 {<<"Content-Type">>, <<"application/octet-stream">>}],
            Ret = buoy:post(BUrl, Headers, NB, TimeOut),
            lager:debug("Influxer batch write ~p ~n -> resp: ~p", [NB, Ret]),
            {noreply, State#state{count=0, buf = <<>>, time = CT}}
    end;
handle_cast({write, Table, Tags, Values}, 
            #state{burl = BUrl, auth_token = AuthToken, timeout = TimeOut} = State) ->
    TagsBin   = lists:foldl(fun influxer_utils:tag_to_bin/2, <<>>, Tags),
    ValuesBin = lists:foldl(fun influxer_utils:tag_to_bin/2, <<>>, Values),
    Body      = <<Table/binary,",", TagsBin/binary, " ", ValuesBin/binary, "\n">>, 
    Headers   = [{<<"Authorization">>, AuthToken},
         {<<"Content-Type">>, <<"application/octet-stream">>}],
    Ret = buoy:post(BUrl, Headers, Body, TimeOut),
    lager:debug("Influxer single write resp: ~p", [Ret]),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, #state{auth_token = AuthToken, timeout = TimeOut, burl = BUrl, buf = Buf} = State) ->
    lager:debug("Info: ~p ~n", [{self(), _Info}]),
    case Buf of
        <<>> -> 
            timer:send_after(?BATCH_TIMEOUT, self(), batch_out),
            {noreply, State}
        ;_   ->
            Headers   = [{<<"Authorization">>, AuthToken},
                 {<<"Content-Type">>, <<"application/octet-stream">>}],
            Ret = buoy:post(BUrl, Headers, Buf, TimeOut),
            lager:debug("HI Influxer batch write ~p ~n -> resp: ~p", [Buf, Ret]),
            CT = erlang:system_time(millisecond),
            timer:send_after(?BATCH_TIMEOUT, self(), batch_out),
            {noreply, State#state{count=0, buf = <<>>, time = CT}}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

