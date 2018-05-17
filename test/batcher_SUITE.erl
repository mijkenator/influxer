-module(batcher_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
            single_write/1,
            get_state/1,
            delete_all/1,
            single_read/1,
            read_nothing/1,
            many_read/1,
            many_write/1
        ]).

init_per_suite(_Config) ->
    ct:get_config(influxer),
    {ok, _} = application:ensure_all_started(influxer),
    %batcher_sup:start_child(b1, [<<"http://10.0.0.85:8086/write?db=mkh_data">>, <<"root">>,<<"Rbccm2018">>,1,2000,1000]),
    {ok, Url} = application:get_env(influxer, influx_url),
    {ok, Lgn} = application:get_env(influxer, influx_lgn),
    {ok, Pwd} = application:get_env(influxer, influx_pwd),
    ct:print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"),
    ct:print(io_lib:format("Config: ~p", [{Url, Lgn, Pwd, application:get_all_env(influxer)}])),
    batcher_sup:start_child(b1, [Url, Lgn, Pwd,1,2000,1000]),
    lager:set_loglevel(lager_console_backend, debug),
    batcher_worker:query(b1, "mkh_data", <<"delete from mkh_test5">>),
    [].

end_per_suite(_Config) ->
    ok = application:stop(influxer).

all() -> [
        delete_all,
        read_nothing,
        single_write,
        single_read,
        get_state,
        many_write,
        many_read
    ].

delete_all(_) ->
    <<"{\"results\":[{\"statement_id\":0}]}\n">> = batcher_worker:query(b1, "mkh_data", <<"delete from mkh_test5">>).

read_nothing(_) ->
    <<"{\"results\":[{\"statement_id\":0}]}\n">> = batcher_worker:query(b1, <<"mkh_data">>, "select value from mkh_test5 where value=3.5").

single_read(_) ->
    Ret  = batcher_worker:query(b1, "mkh_data", "select value from mkh_test5 limit 1"),
    ct:print(io_lib:format("SR: ~p ~n", [Ret])),
    Json = jsx:decode(Ret),
    [Results]  = proplists:get_value(<<"results">>, Json),
    [Series]   = proplists:get_value(<<"series">>, Results),
    [[_, 1.5]] = proplists:get_value(<<"values">>, Series).

many_read(_) ->
    Ret  = batcher_worker:query(b1, "mkh_data", "select value from mkh_test5 limit 3"),
    ct:print(io_lib:format("SR: ~p ~n", [Ret])),
    Json = jsx:decode(Ret),
    [Results]  = proplists:get_value(<<"results">>, Json),
    [Series]   = proplists:get_value(<<"series">>, Results),
    [[_, 1.5],[_, 1.5],[_, 1.5]] = proplists:get_value(<<"values">>, Series).


get_state(_) -> 
    State = {state,0,0,<<>>,<<"Basic cm9vdDpSYmNjbTIwMTg=">>,
           {buoy_url,<<"10.0.0.85:8086">>,<<"10.0.0.85">>,<<"/write?db=mkh_data">>,8086,http},
            2000,5000,1000,
           {buoy_url,<<"10.0.0.85:8086">>,<<"10.0.0.85">>,<<"/query">>,8086,http}},
    
    State = batcher_worker:get_state(b1).

single_write(_Config) ->
    ok = batcher_worker:write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}],[{<<"value">>, 1.5},{<<"average">>, 2.5},{<<"val2">>, 3.5}]}).

many_write(_Config) ->
    ok = batcher_worker:batch_write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}],[{<<"value">>, 1.5},{<<"average">>, 2.5},{<<"val2">>, 3.5}]}),
    ok = batcher_worker:batch_write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}],[{<<"value">>, 1.5},{<<"average">>, 2.5},{<<"val2">>, 3.5}]}),
    ok = batcher_worker:batch_write(b1, {<<"mkh_test5">>, [{<<"tag1">>,<<"t1">>},{<<"tag2">>, <<"t2">>}],[{<<"value">>, 1.5},{<<"average">>, 2.5},{<<"val2">>, 3.5}]}),
    timer:sleep(5500),
    ok=ok.

