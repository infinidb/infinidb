TARGETS = bootstrap clean test docs coverage leakcheck install

.PHONY: $(TARGETS)

PROGS = hdfs-20 hdfs-12

all $(TARGETS):
	if [ $@ != all ]; \
		then target=$@; \
	fi; \
	for prog in $(PROGS); \
		do $(MAKE) -C $$prog $$target || exit $$?; \
	done

