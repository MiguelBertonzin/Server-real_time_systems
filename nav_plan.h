#ifndef NAV_PLAN_H
#define NAV_PLAN_H

typedef struct {
    float roll_deg;
    float pitch_deg;
    float yaw_deg;
} Waypoint;

// Avança o índice circular da rota e devolve o waypoint selecionado.
Waypoint nav_plan_avancar(unsigned int *indice);
float nav_plan_calcular_carga(float semente);

#endif
