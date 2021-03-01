#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

void fatal(const char *file, int line) {
    if (errno != 0) {
        fprintf(stderr, "Error: %s at %s, line %d.\n", strerror(errno), file, line);
    } else {
        fprintf(stderr, "Error at %s, line %d.\n", file, line);
    }
    exit(EXIT_FAILURE);
}
