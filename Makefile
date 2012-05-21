# edis Makefile
# Copyright (C) 2011 Electronic Inaka, LLC <contact at inakanetworks dot com>
# edis is licensed by Electronic Inaka, LLC under the Apache 2.0 license

ERL := erl -pa deps/*/ebin -pa ebin -pa src -boot start_sasl +Bc +K true -smp enable -s crypto -s inets -s ssl -s elog ${ERL_ARGS}
PREFIX= /usr/local
INSTALL_BIN= $(PREFIX)/bin
INSTALL= cp -p

all: erl
	mkdir -p bin
	./priv/script_builder

erl:
	rebar get-deps && rebar compile

clean:
	rm -rf bin
	rebar clean

build_plt: erl
	dialyzer --verbose --build_plt --apps kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
				    xmerl webtool snmp public_key mnesia eunit syntax_tools compiler --output_plt ~/.edis_plt -pa deps/*/ebin ebin

analyze: erl
	dialyzer --verbose -pa deps/*/ebin --plt ~/.edis_plt -Wunmatched_returns -Werror_handling -Wbehaviours ebin

xref: erl
	rebar skip_deps=true xref

run:
	${ERL} -s edis

test: erl
	${ERL} -config test.config -noshell -sname edis_test_server -s edis -run elog debug &
	mkdir -p ./test/ebin
	erlc -o ./test/ebin +debug_info ./test/*_SUITE.erl
	rebar skip_deps=true ct ; \
	kill `ps aux | grep beam | grep edis_[t]est_server | awk '{print $$2}'`

shell: erl
	${ERL}

doc: erl
	cd deps/erldocs
	make
	cd ../..
	./deps/erldocs/erldocs doc
	cat doc/erldocs_index.js | tr -d '\n' > doc/erldocs_index2.js
	mv ./doc/erldocs_index2.js ./doc/erldocs_index.js

install:
	mkdir -p $(INSTALL_BIN)
	$(INSTALL) bin/* $(INSTALL_BIN)

service: install
	mkdir -p /etc/edis/db/
	if [ ! -f /etc/edis/edis.config ] ; then cp priv/edis.config /etc/edis/ ; fi
	cp priv/edis.init.d /etc/init.d/edis

