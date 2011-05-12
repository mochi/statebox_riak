REBAR=./rebar

all:
	@$(REBAR) get-deps compile

edoc:
	@$(REBAR) skip_deps=true doc

test:
	@rm -rf .eunit
	@mkdir -p .eunit
	@$(REBAR) skip_deps=true eunit

clean:
	@$(REBAR) clean

build_plt:
	@$(REBAR) skip_deps=true build-plt

dialyzer:
	@$(REBAR) skip_deps=true dialyze
