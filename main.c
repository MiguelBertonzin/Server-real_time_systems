#define _POSIX_C_SOURCE 200809L

#include "nav_plan.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define NS_POR_MS INT64_C(1000000)
#define NS_POR_S INT64_C(1000000000)

enum {
    DURACAO_S = 10,
    PERIODO_MS = 50,
    ORCAMENTO_MS = 20,
    RESERVA_NAV_MS = 5,
    CARGA_NAV_MS = 4,
    CARGA_TAREFA_MS = 5,
    PRAZO_NAV_MS = 20,
    INTERVALO_EXIBICAO_MS = 400,
    TOTAL_TAREFAS = 4,
    PERIODO_T1_MS = 40,
    PRIORIDADE_SERVIDOR = 60,
    PRIORIDADE_TAREFA = 50
};

// Rajada para encher a fila e pedidos espaçados até o fim da execução.
static const int chegadas_ms[] = {
    5, 8, 11, 14, 17, 41, 44, 75, 144, 193,
    443, 794, 1143, 1463, 1794, 2143, 2463, 2794,
    3143, 3463, 3794, 4143, 4463, 4794, 5143, 5463, 5794,
    6143, 6463, 6794, 7143, 7463, 7794, 8143, 8463, 8794,
    9143, 9463, 9794
};
enum {
    TOTAL_PEDIDOS = sizeof(chegadas_ms) / sizeof(chegadas_ms[0]),
    TOTAL_JANELAS = (DURACAO_S * 1000 + PERIODO_MS - 1) / PERIODO_MS,
    // Uma linha por ativação, duas por pedido e até uma pausa por janela.
    CAPACIDADE_EVENTOS = TOTAL_TAREFAS * ((DURACAO_S * 1000 + PERIODO_T1_MS - 1) /
                                        PERIODO_T1_MS)
                       + 2 * TOTAL_PEDIDOS + TOTAL_JANELAS
};

typedef struct {
    unsigned int id;
    int64_t chegada;
} Pedido;

typedef struct {
    Pedido pedido;
    Waypoint ponto;
    unsigned int waypoint;
    unsigned int janela;
    int64_t inicio;
    int64_t fim;
    int64_t cpu;
} Resultado;

typedef struct {
    enum { ATIVACAO, CHEGADA, ATENDIMENTO, PAUSA } tipo;
    int64_t instante;
    union {
        struct {
            const char *nome;
            unsigned int numero;
        } ativacao;
        struct {
            unsigned int id;
            size_t pendentes;
        } chegada;
        const Resultado *atendimento;
        struct {
            unsigned int janela;
            size_t pendentes;
            int64_t cpu;
            int64_t proxima;
            bool limite_cpu;
        } pausa;
    };
} Evento;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t evento_pronto;
    Pedido fila[TOTAL_PEDIDOS];
    size_t primeiro;
    size_t proximo;
    Resultado resultados[TOTAL_PEDIDOS];
    Evento eventos[CAPACIDADE_EVENTOS];
    size_t total_eventos;
    bool terminou;
    bool detalhado;
    bool usar_cor;
    int64_t cpu_maxima_janela;
    int64_t inicio;
    int64_t fim;
} Execucao;

typedef struct {
    const char *nome;
    int periodo_ms;
    Execucao *execucao;
    unsigned int ativacoes;
} Tarefa;

static double em_ms(int64_t tempo)
{
    return (double)tempo / NS_POR_MS;
}

static int64_t tempo_ns(clockid_t relogio)
{
    struct timespec tempo;
    if (clock_gettime(relogio, &tempo) != 0) {
        perror("clock_gettime");
        exit(EXIT_FAILURE);
    }
    return (int64_t)tempo.tv_sec * NS_POR_S + tempo.tv_nsec;
}

static void dormir_ate(int64_t instante)
{
    struct timespec tempo = {
        .tv_sec = instante / NS_POR_S,
        .tv_nsec = instante % NS_POR_S
    };
    int erro;
    do {
        erro = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &tempo, NULL);
    } while (erro == EINTR);
    if (erro != 0) {
        fprintf(stderr, "clock_nanosleep: %s\n", strerror(erro));
        exit(EXIT_FAILURE);
    }
}

// Chamado com o mutex adquirido, para publicar o evento junto com a alteração da fila.
static void publicar_evento(Execucao *execucao, Evento evento)
{
    assert(execucao->total_eventos < CAPACIDADE_EVENTOS);
    evento.instante = tempo_ns(CLOCK_MONOTONIC);
    execucao->eventos[execucao->total_eventos++] = evento;
    pthread_cond_signal(&execucao->evento_pronto);
}

static void registrar_evento(Execucao *execucao, Evento evento)
{
    pthread_mutex_lock(&execucao->mutex);
    publicar_evento(execucao, evento);
    pthread_mutex_unlock(&execucao->mutex);
}

static void registrar_pausa(Execucao *execucao, unsigned int janela, int64_t cpu,
                            int64_t proxima, bool limite_cpu)
{
    pthread_mutex_lock(&execucao->mutex);
    size_t pendentes = execucao->proximo - execucao->primeiro;
    if (pendentes > 0) {
        publicar_evento(execucao, (Evento){
            .tipo = PAUSA,
            .pausa = {janela, pendentes, cpu, proxima, limite_cpu}
        });
    }
    pthread_mutex_unlock(&execucao->mutex);
}

static void enfileirar(Execucao *execucao, unsigned int id)
{
    pthread_mutex_lock(&execucao->mutex);
    Pedido pedido = {
        .id = id,
        .chegada = tempo_ns(CLOCK_MONOTONIC)
    };
    execucao->fila[execucao->proximo++] = pedido;
    publicar_evento(execucao, (Evento){
        .tipo = CHEGADA,
        .chegada = {id, execucao->proximo - execucao->primeiro}
    });
    pthread_mutex_unlock(&execucao->mutex);
}

static int retirar(Execucao *execucao, Pedido *pedido)
{
    pthread_mutex_lock(&execucao->mutex);
    int tem_pedido = execucao->primeiro < execucao->proximo;
    if (tem_pedido)
        *pedido = execucao->fila[execucao->primeiro++];
    pthread_mutex_unlock(&execucao->mutex);
    return tem_pedido;
}

static void ocupar_cpu(int duracao_ms, float semente)
{
    int64_t inicio = tempo_ns(CLOCK_THREAD_CPUTIME_ID);
    do {
        semente = nav_plan_calcular_carga(semente);
    } while (tempo_ns(CLOCK_THREAD_CPUTIME_ID) - inicio < duracao_ms * NS_POR_MS);
}

static void atender(Execucao *execucao, Pedido pedido, unsigned int janela,
                    unsigned int *indice_waypoint)
{
    Resultado *resultado = &execucao->resultados[pedido.id - 1];
    resultado->pedido = pedido;
    resultado->janela = janela;
    resultado->inicio = tempo_ns(CLOCK_MONOTONIC);

    int64_t cpu_inicio = tempo_ns(CLOCK_THREAD_CPUTIME_ID);
    ocupar_cpu(CARGA_NAV_MS, (float)pedido.id);
    resultado->ponto = nav_plan_avancar(indice_waypoint);
    resultado->waypoint = *indice_waypoint;
    resultado->cpu = tempo_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_inicio;
    resultado->fim = tempo_ns(CLOCK_MONOTONIC);
    registrar_evento(execucao, (Evento){.tipo = ATENDIMENTO, .atendimento = resultado});
}

static void *tarefa_periodica(void *arg)
{
    Tarefa *tarefa = arg;
    Execucao *execucao = tarefa->execucao;
    const int64_t periodo = tarefa->periodo_ms * NS_POR_MS;
    int64_t liberacao = execucao->inicio;
    int64_t proxima_exibicao = execucao->inicio;

    while (liberacao < execucao->fim) {
        dormir_ate(liberacao);
        int64_t agora = tempo_ns(CLOCK_MONOTONIC);
        if (agora >= execucao->fim)
            break;

        ++tarefa->ativacoes;
        if (execucao->detalhado || agora >= proxima_exibicao) {
            registrar_evento(execucao, (Evento){
                .tipo = ATIVACAO,
                .ativacao = {tarefa->nome, tarefa->ativacoes}
            });
            proxima_exibicao = agora + INTERVALO_EXIBICAO_MS * NS_POR_MS;
        }
        ocupar_cpu(CARGA_TAREFA_MS, (float)tarefa->periodo_ms);

        // Se houve atraso, segue a agenda em vez de repetir ativações vencidas.
        liberacao += ((agora - liberacao) / periodo + 1) * periodo;
    }
    return NULL;
}

static void *servidor(void *arg)
{
    Execucao *execucao = arg;
    unsigned int indice_waypoint = 0;
    int64_t liberacao = execucao->inicio;
    const int64_t periodo = PERIODO_MS * NS_POR_MS;
    const int64_t orcamento = ORCAMENTO_MS * NS_POR_MS;
    const int64_t reserva = RESERVA_NAV_MS * NS_POR_MS;

    while (liberacao < execucao->fim) {
        dormir_ate(liberacao);
        int64_t agora = tempo_ns(CLOCK_MONOTONIC);
        if (agora >= execucao->fim)
            break;

        liberacao += (agora - liberacao) / periodo * periodo;
        unsigned int janela = (unsigned int)((liberacao - execucao->inicio) / periodo);
        int64_t proxima = liberacao + periodo;
        int64_t cpu_inicio = tempo_ns(CLOCK_THREAD_CPUTIME_ID);

        // Só retira um pedido quando ainda cabe a reserva de atendimento.
        while (true) {
            int64_t gasto = tempo_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_inicio;
            bool limite_cpu = gasto + reserva > orcamento;
            if (limite_cpu || tempo_ns(CLOCK_MONOTONIC) + reserva > proxima) {
                registrar_pausa(execucao, janela, gasto, proxima, limite_cpu);
                break;
            }
            Pedido pedido;
            if (!retirar(execucao, &pedido))
                break;

            atender(execucao, pedido, janela, &indice_waypoint);
        }
        int64_t gasto = tempo_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_inicio;
        if (gasto > execucao->cpu_maxima_janela)
            execucao->cpu_maxima_janela = gasto;
        liberacao = proxima;
    }
    return NULL;
}

static int criar_thread(pthread_t *thread, void *(*funcao)(void *), void *arg,
                        bool tempo_real, int prioridade)
{
    pthread_attr_t attr;
    int erro = pthread_attr_init(&attr);
    if (erro != 0)
        return erro;

    int politica = tempo_real ? SCHED_FIFO : SCHED_OTHER;
    struct sched_param param = {
        .sched_priority = tempo_real ? prioridade : 0
    };
    erro = pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
    if (erro == 0)
        erro = pthread_attr_setschedpolicy(&attr, politica);
    if (erro == 0)
        erro = pthread_attr_setschedparam(&attr, &param);
    if (erro == 0)
        erro = pthread_create(thread, &attr, funcao, arg);
    pthread_attr_destroy(&attr);
    return erro;
}

static void imprimir_evento(const Execucao *execucao, Evento evento)
{
    const char *normal = execucao->usar_cor ? "\033[0m" : "";
    const char *ciano = execucao->usar_cor ? "\033[1;36m" : "";
    const char *amarelo = execucao->usar_cor ? "\033[1;33m" : "";
    const char *verde = execucao->usar_cor ? "\033[1;32m" : "";
    double tempo = em_ms(evento.instante - execucao->inicio);

    switch (evento.tipo) {
    case ATIVACAO:
        printf("%8.2f ms  [%-2s]       executando (ativação %u)\n",
               tempo, evento.ativacao.nome, evento.ativacao.numero);
        break;
    case CHEGADA:
        printf("%8.2f ms  %s[GERADOR]%s  NAV %02u entrou | fila %zu\n",
               tempo, amarelo, normal, evento.chegada.id, evento.chegada.pendentes);
        break;
    case ATENDIMENTO: {
        const Resultado *r = evento.atendimento;
        int64_t resposta = r->fim - r->pedido.chegada;
        bool cumpriu = resposta <= PRAZO_NAV_MS * NS_POR_MS;
        printf("%8.2f ms  %s[SERVIDOR]%s NAV %02u | janela %03u | WP %u | resposta %6.2f ms | %s%s%s\n",
               tempo, ciano, normal, r->pedido.id, r->janela, r->waypoint, em_ms(resposta),
               cumpriu ? verde : amarelo, cumpriu ? "NO PRAZO" : "ATRASO", normal);
        break;
    }
    case PAUSA:
        printf("%8.2f ms  %s[JANELA]%s   %03u pausou por %s | CPU %.2f/%d ms | fila %zu | próxima %.0f ms\n",
               tempo, ciano, normal, evento.pausa.janela,
               evento.pausa.limite_cpu ? "orçamento" : "fim da janela",
               em_ms(evento.pausa.cpu), ORCAMENTO_MS, evento.pausa.pendentes,
               em_ms(evento.pausa.proxima - execucao->inicio));
        break;
    }
}

static void *exibir_eventos(void *arg)
{
    Execucao *execucao = arg;
    size_t leitura = 0;

    for (;;) {
        pthread_mutex_lock(&execucao->mutex);
        while (leitura == execucao->total_eventos && !execucao->terminou)
            pthread_cond_wait(&execucao->evento_pronto, &execucao->mutex);
        if (leitura == execucao->total_eventos) {
            pthread_mutex_unlock(&execucao->mutex);
            break;
        }
        Evento evento = execucao->eventos[leitura++];
        pthread_mutex_unlock(&execucao->mutex);
        imprimir_evento(execucao, evento);
    }
    return NULL;
}

static void imprimir_tabela(const Execucao *execucao)
{
    puts("\nNAV_PLAN — Tempos em ms desde o início. Janela 0 começa em 0 ms.\n");
    puts(" ID  Janela  Chegada   Início      Fim    CPU  Resposta  Prazo    WP  (roll, pitch, yaw)");

    for (size_t i = 0; i < execucao->primeiro; ++i) {
        const Resultado *r = &execucao->resultados[i];
        int64_t resposta = r->fim - r->pedido.chegada;
        bool cumpriu = resposta <= PRAZO_NAV_MS * NS_POR_MS;
        printf("%3u %7u %8.2f %8.2f %8.2f %6.2f %9.2f  %-7s %2u  (%g, %g, %g)\n",
               r->pedido.id, r->janela, em_ms(r->pedido.chegada - execucao->inicio),
               em_ms(r->inicio - execucao->inicio), em_ms(r->fim - execucao->inicio),
               em_ms(r->cpu), em_ms(resposta), cumpriu ? "ok" : "perdido",
               r->waypoint, r->ponto.roll_deg, r->ponto.pitch_deg, r->ponto.yaw_deg);
    }
}

static void imprimir_resultados(const Execucao *execucao, const Tarefa tarefas[])
{
    if (execucao->detalhado)
        imprimir_tabela(execucao);

    size_t no_prazo = 0;
    int64_t resposta_minima = INT64_MAX;
    int64_t resposta_maxima = 0;
    int64_t soma_respostas = 0;
    unsigned int pedido_mais_lento = 0;
    for (size_t i = 0; i < execucao->primeiro; ++i) {
        const Resultado *r = &execucao->resultados[i];
        int64_t resposta = r->fim - r->pedido.chegada;
        no_prazo += resposta <= PRAZO_NAV_MS * NS_POR_MS;
        if (resposta < resposta_minima)
            resposta_minima = resposta;
        if (resposta > resposta_maxima) {
            resposta_maxima = resposta;
            pedido_mais_lento = r->pedido.id;
        }
        soma_respostas += resposta;
    }

    printf("\nNAV_PLAN | %zu/%zu atendidos | %zu no prazo | %zu em atraso | %zu na fila\n",
           execucao->primeiro, execucao->proximo, no_prazo, execucao->primeiro - no_prazo,
           execucao->proximo - execucao->primeiro);
    if (execucao->primeiro > 0) {
        printf("Resposta | mín %.2f ms | média %.2f ms | máx %.2f ms (NAV %02u)\n",
               em_ms(resposta_minima), em_ms(soma_respostas) / (double)execucao->primeiro,
               em_ms(resposta_maxima), pedido_mais_lento);
    }
    printf("CPU máxima por janela | %.2f de %d ms\n",
           em_ms(execucao->cpu_maxima_janela), ORCAMENTO_MS);
    printf("Ativações periódicas");
    for (size_t i = 0; i < TOTAL_TAREFAS; ++i)
        printf(" | %s %u", tarefas[i].nome, tarefas[i].ativacoes);
    putchar('\n');
}

static int iniciar_mutex(pthread_mutex_t *mutex)
{
    pthread_mutexattr_t attr;
    int erro = pthread_mutexattr_init(&attr);
    if (erro != 0)
        return erro;
    erro = pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT);
    if (erro == 0)
        erro = pthread_mutex_init(mutex, &attr);
    pthread_mutexattr_destroy(&attr);
    return erro;
}

int main(int argc, char **argv)
{
    bool tempo_real = false;
    bool detalhado = false;
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--rt") == 0)
            tempo_real = true;
        else if (strcmp(argv[i], "--detalhado") == 0)
            detalhado = true;
        else {
            fprintf(stderr, "Uso: %s [--rt] [--detalhado]\n", argv[0]);
            return EXIT_FAILURE;
        }
    }

    setvbuf(stdout, NULL, _IOLBF, BUFSIZ);
    int64_t inicio = tempo_ns(CLOCK_MONOTONIC) + 100 * NS_POR_MS;
    Execucao execucao = {
        .evento_pronto = PTHREAD_COND_INITIALIZER,
        .detalhado = detalhado,
        .usar_cor = isatty(STDOUT_FILENO) && getenv("NO_COLOR") == NULL,
        .inicio = inicio,
        .fim = inicio + DURACAO_S * NS_POR_S
    };
    Tarefa tarefas[TOTAL_TAREFAS] = {
        {"T1", PERIODO_T1_MS, &execucao, 0},
        {"T2", 80, &execucao, 0},
        {"T3", 120, &execucao, 0},
        {"T4", 160, &execucao, 0}
    };
    pthread_t threads[1 + TOTAL_TAREFAS];

    int erro = iniciar_mutex(&execucao.mutex);
    if (erro != 0) {
        fprintf(stderr, "Falha ao inicializar o mutex: %s\n", strerror(erro));
        return EXIT_FAILURE;
    }
    erro = criar_thread(&threads[0], servidor, &execucao, tempo_real, PRIORIDADE_SERVIDOR);
    if (erro != 0) {
        fprintf(stderr, "Falha ao criar o servidor com %s: %s\n",
                tempo_real ? "SCHED_FIFO" : "SCHED_OTHER", strerror(erro));
        return EXIT_FAILURE;
    }

    int politica;
    struct sched_param param;
    erro = pthread_getschedparam(threads[0], &politica, &param);
    if (erro != 0) {
        fprintf(stderr, "pthread_getschedparam: %s\n", strerror(erro));
        return EXIT_FAILURE;
    }
    printf("NAV_PLAN | %s prioridade %d | Ts=%d ms | Cs=%d ms | prazo=%d ms | duração=%d s\n",
           politica == SCHED_FIFO ? "SCHED_FIFO" : "SCHED_OTHER",
           param.sched_priority, PERIODO_MS, ORCAMENTO_MS, PRAZO_NAV_MS, DURACAO_S);
    printf("Reserva para iniciar um NAV | %d ms\n", RESERVA_NAV_MS);
    printf("Períodos das tarefas");
    for (size_t i = 0; i < TOTAL_TAREFAS; ++i)
        printf(" | %s %d ms", tarefas[i].nome, tarefas[i].periodo_ms);
    puts("");
    if (!detalhado)
        printf("Ativações exibidas a cada %d ms ou mais por tarefa.\n", INTERVALO_EXIBICAO_MS);
    putchar('\n');

    for (size_t i = 0; i < TOTAL_TAREFAS; ++i) {
        erro = criar_thread(&threads[i + 1], tarefa_periodica, &tarefas[i],
                            tempo_real, PRIORIDADE_TAREFA);
        if (erro != 0) {
            fprintf(stderr, "Falha ao criar %s: %s\n", tarefas[i].nome, strerror(erro));
            return EXIT_FAILURE;
        }
    }

    pthread_t exibicao;
    erro = criar_thread(&exibicao, exibir_eventos, &execucao, false, 0);
    if (erro != 0) {
        fprintf(stderr, "Falha ao criar a thread de exibição: %s\n", strerror(erro));
        return EXIT_FAILURE;
    }

    for (unsigned int i = 0; i < TOTAL_PEDIDOS; ++i) {
        dormir_ate(execucao.inicio + chegadas_ms[i] * NS_POR_MS);
        if (tempo_ns(CLOCK_MONOTONIC) >= execucao.fim)
            break;
        enfileirar(&execucao, i + 1);
    }

    dormir_ate(execucao.fim);
    for (size_t i = 0; i < 1 + TOTAL_TAREFAS; ++i) {
        erro = pthread_join(threads[i], NULL);
        if (erro != 0) {
            fprintf(stderr, "pthread_join: %s\n", strerror(erro));
            return EXIT_FAILURE;
        }
    }
    pthread_mutex_lock(&execucao.mutex);
    execucao.terminou = true;
    pthread_cond_signal(&execucao.evento_pronto);
    pthread_mutex_unlock(&execucao.mutex);
    erro = pthread_join(exibicao, NULL);
    if (erro != 0) {
        fprintf(stderr, "pthread_join: %s\n", strerror(erro));
        return EXIT_FAILURE;
    }
    pthread_cond_destroy(&execucao.evento_pronto);
    pthread_mutex_destroy(&execucao.mutex);
    imprimir_resultados(&execucao, tarefas);
    return EXIT_SUCCESS;
}
