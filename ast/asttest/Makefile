# List any generated files here
TARGETS = cmpopts.go
# List any source files used to generate the targets here
SOURCES = ../ast.go gen.go ../../bin/$(GOOS)/cmpgen
# List any directories that have their own Makefile here
SUBDIRS =

# Default target
generate: $(SUBDIRS) $(TARGETS)

# Recurse into subdirs for same make goal
$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

# Clean all targets recursively
clean: $(SUBDIRS)
	rm -f $(TARGETS)

# Define go generate if not already defined
GO_GENERATE := go generate

# Run go generate for the targets
$(TARGETS): $(SOURCES)
	PATH=../../bin/${GOOS}:${PATH} $(GO_GENERATE) -x

.PHONY: generate clean $(SUBDIRS)
