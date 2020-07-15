.PHONY: initialize-db

initialize-db:
	psql -d ${BRAINPOP_DB_NAME} -U ${BRAINPOP_DB_USERNAME} -f initialize_db.sql