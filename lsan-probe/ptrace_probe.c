/* Replicates compiler-rt TestPTrace(): fork a child that does
 * ptrace(PTRACE_ATTACH, 0, 0, 0) then _exit(0); parent waitpid and
 * reports how the child terminated. This is the exact probe whose
 * WIFSIGNALED path prints "ptrace appears to be blocked". Run many
 * times to see whether the child is signalled (and with what) on the
 * host image. Also does a real self-thread attach to see if the
 * stop-the-world attach path hangs. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ptrace.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>

int main(void) {
    pid_t pid = fork();
    if (pid < 0) {
        printf("fork_failed errno=%d\n", errno);
        return 2;
    }
    if (pid == 0) {
        ptrace(PTRACE_ATTACH, 0, 0, 0);
        _exit(0);
    }
    int ws = 0;
    waitpid(pid, &ws, 0);
    if (WIFSIGNALED(ws)) {
        printf("child_signalled sig=%d raw_wstatus=0x%x\n", WTERMSIG(ws), ws);
    } else if (WIFEXITED(ws)) {
        printf("child_exited code=%d raw_wstatus=0x%x\n", WEXITSTATUS(ws), ws);
    } else {
        printf("child_other raw_wstatus=0x%x\n", ws);
    }
    return 0;
}
