help: ## Show this help message.
	@echo
	@echo 'usage: make [target]'
	@echo
	@echo 'targets:'
	@echo
	@egrep '^(.+)\:\ ##\ (.+)' ${MAKEFILE_LIST} | column -t -c 2 -s ':#'
	@echo
.PHONY: help

#
# Initialize
#

init: ## Initialize the project.
	touch docker-compose.local.yml

#
# Test
#

test: ## Test the project.
test: | _include_env
	@echo "Testing..."
	@mkdir -p coverage
	@go test \
		-cover \
		-covermode=atomic \
		-coverprofile coverage/coverage.out \
		-count=1 \
		-failfast \
		./...

	@echo "Generating coverage report..."
	@go tool cover \
		-html=coverage/coverage.out \
		-o coverage/coverage.html

	@echo "Dropping test databases..."
	@set -e; for dbname in $$(psql "${DATABASE_URL}" -c "copy (select datname from pg_database where datname like 'test-%') to stdout") ; do \
		psql "${DATABASE_URL}" -q -c "DROP DATABASE \"$$dbname\"" ; \
	done

#
# Dependencies
#

tidy: ## Tidy the go modules.
tidy: | _include_env
	@go mod tidy
.PHONY: tidy

update: ## Update the go modules.
update: | _include_env
	@go get -u ./...
	@go mod tidy
.PHONY: update

#
# Postgres
#

psql: ## Connect to the postgres database.
psql: | _include_env
	@echo "Connecting to postgres"
	psql "${DATABASE_URL}"
.PHONY: psql

database: ## Migrate the database.
database: | _include_env
	@echo "Migrating database"
	@dbmate -u ${DATABASE_URL} \
		--migrations-table pubsub_schema_migrations \
		--migrations-dir db/migrations \
		--schema-file db/schema.sql \
		up
.PHONY: database

database-clean: ## Drop the database and recreate it.
database-clean: | _include_env _database-drop database
.PHONY: database-clean

_database-drop: | _include_env
	@dbmate -u ${DATABASE_URL} drop
.PHONY: _database-drop

#
# Helpers
#

_include_env:
	@$(eval include .env)
	@$(eval export)
.PHONY: _include_env
