CC = gcc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -Wpedantic

.PHONY: all clean
all: servidor_m3

servidor_m3: main.c nav_plan.c nav_plan.h
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) -pthread main.c nav_plan.c -o $@ $(LDLIBS)

clean:
	rm -f servidor_m3 servidor_m3.exe
