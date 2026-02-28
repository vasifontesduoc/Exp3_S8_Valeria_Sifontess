------------------------------------------------------
-- SOLUCIONES A LA ACTIVIDAD PLANTEADA
-- SUMATIVA 3
-- Valeria Sifontes
------------------------------------------------------

------------------------------------------------------
-- CASO 1
------------------------------------------------------

-- TRIGGER
-- mantiene sincronizada la tabla TOTAL_CONSUMOS
-- cada vez que se inserta actualiza o elimina
-- un registro en consumo
CREATE OR REPLACE TRIGGER trg_total_consumos
AFTER INSERT OR UPDATE OR DELETE ON consumo
FOR EACH ROW
BEGIN

   -- si se inserta un consumo se suma al total
   IF INSERTING THEN
      UPDATE total_consumos
      SET monto_consumos = NVL(monto_consumos,0) + :NEW.monto
      WHERE id_huesped = :NEW.id_huesped;

   -- si se actualiza se ajusta la diferencia
   ELSIF UPDATING THEN
      UPDATE total_consumos
      SET monto_consumos = NVL(monto_consumos,0)
                           - :OLD.monto
                           + :NEW.monto
      WHERE id_huesped = :NEW.id_huesped;

   -- si se elimina se descuenta del total
   ELSIF DELETING THEN
      UPDATE total_consumos
      SET monto_consumos = NVL(monto_consumos,0) - :OLD.monto
      WHERE id_huesped = :OLD.id_huesped;

   END IF;

END;
/


------------------------------------------------------
-- CASO 2
------------------------------------------------------

CREATE OR REPLACE PACKAGE pkg_cobranza AS

   -- variable pública que almacena el monto
   -- total en dólares por concepto de tours
   v_monto_tours NUMBER;

   -- función que retorna el total de tours
   -- en dólares para un huésped
   FUNCTION fn_monto_tours (
      p_id_huesped NUMBER
   ) RETURN NUMBER;

END pkg_cobranza;
/

----------------------------------------------------
-- PACKAGE BODY

CREATE OR REPLACE PACKAGE BODY pkg_cobranza AS

   FUNCTION fn_monto_tours (
      p_id_huesped NUMBER
   ) RETURN NUMBER
   IS
      v_total NUMBER := 0;
   BEGIN

      -- se calcula:
      -- valor del tour * cantidad de personas
      -- todos los valores están en dólares
      SELECT NVL(SUM(t.valor_tour * ht.num_personas),0)
      INTO v_total
      FROM huesped_tour ht
      JOIN tour t ON ht.id_tour = t.id_tour
      WHERE ht.id_huesped = p_id_huesped;

      v_monto_tours := ROUND(v_total);
      RETURN v_monto_tours;

   EXCEPTION
   -- si ocurre cualquier error retorna 0
      WHEN NO_DATA_FOUND THEN
         RETURN 0;
      WHEN OTHERS THEN
         RETURN 0;
   END;

END pkg_cobranza;
/

----------------------------------------------------
-- FUNCIÓN AGENCIA
-- retorna el nombre de la agencia del huésped
-- si no tiene agencia registra error en REG_ERRORES
-- usando secuencia SQ_ERROR

CREATE OR REPLACE FUNCTION fn_agencia_huesped (
   p_id_huesped NUMBER
) RETURN VARCHAR2
IS
   PRAGMA AUTONOMOUS_TRANSACTION;

   v_agencia   agencia.nom_agencia%TYPE;
   v_id_error  NUMBER;
   v_msg_error VARCHAR2(300);

BEGIN
    -- se obtiene agencia asociada al huésped
   SELECT a.nom_agencia
   INTO v_agencia
   FROM huesped h
   JOIN agencia a ON h.id_agencia = a.id_agencia
   WHERE h.id_huesped = p_id_huesped;

   RETURN v_agencia;

EXCEPTION
   WHEN NO_DATA_FOUND THEN
    -- se registra el error en tabla REG_ERRORES
      v_msg_error := SQLERRM;
      SELECT sq_error.NEXTVAL INTO v_id_error FROM dual;

      INSERT INTO reg_errores (
         id_error,
         nomsubprograma,
         msg_error
      )
      VALUES (
         v_id_error,
         'fn_agencia_huesped',
         v_msg_error
      );

      COMMIT;
      RETURN 'NO REGISTRA AGENCIA';

   WHEN OTHERS THEN

      v_msg_error := SQLERRM;
      SELECT sq_error.NEXTVAL INTO v_id_error FROM dual;

      INSERT INTO reg_errores (
         id_error,
         nomsubprograma,
         msg_error
      )
      VALUES (
         v_id_error,
         'fn_agencia_huesped',
         v_msg_error
      );

      COMMIT;
      RETURN 'NO REGISTRA AGENCIA';

END;
/

------------------------------------------------------
-- FUNCIÓN FN_MONTO_CONSUMOS
-- obtiene el total acumulado de consumos del huésped
-- desde la tabla TOTAL_CONSUMOS
-- si no existen consumos retorna 0

CREATE OR REPLACE FUNCTION fn_monto_consumos (
   p_id_huesped NUMBER
) RETURN NUMBER
IS
   v_total NUMBER := 0;
BEGIN

   SELECT NVL(monto_consumos,0)
   INTO v_total
   FROM total_consumos
   WHERE id_huesped = p_id_huesped;

   RETURN ROUND(v_total);

EXCEPTION
   WHEN NO_DATA_FOUND THEN
      RETURN 0;
END;
/

------------------------------------------------------
-- FUNCIÓN FN_CALCULAR_ALOJAMIENTO
-- calcula:
-- (valor habitación + valor minibar) * días estadía
-- se crea como subprograma adicional para mejorar
-- modularidad y reutilización


CREATE OR REPLACE FUNCTION fn_calcular_alojamiento (
   p_id_reserva NUMBER,
   p_estadia    NUMBER
) RETURN NUMBER
IS
   v_total NUMBER := 0;
BEGIN

   SELECT NVL(SUM((ha.valor_habitacion + ha.valor_minibar)
                  * p_estadia),0)
   INTO v_total
   FROM detalle_reserva dr
   JOIN habitacion ha
     ON dr.id_habitacion = ha.id_habitacion
   WHERE dr.id_reserva = p_id_reserva;

   RETURN ROUND(v_total);

EXCEPTION
   WHEN OTHERS THEN
      RETURN 0;
END;
/

------------------------------------------------------
-- FUNCIÓN FN_DESCUENTO_AGENCIA
-- aplica descuento del 12% si la agencia
-- corresponde a VIAJES ALBERTI

CREATE OR REPLACE FUNCTION fn_descuento_agencia (
   p_agencia VARCHAR2,
   p_base    NUMBER
) RETURN NUMBER
IS
BEGIN
   IF UPPER(TRIM(p_agencia)) = 'VIAJES ALBERTI' THEN
      RETURN ROUND(p_base * 0.12);
   ELSE
      RETURN 0;
   END IF;
END;
/

------------------------------------------------------
-- PROCEDIMIENTO SP_PROCESO_COBRANZA
-- p_fecha_proceso - fecha a procesar 
-- p_valor_dolar   - tipo de cambio 
-- procedimiento:
-- 1) procesa huéspedes cuya fecha de salida
--    coincide con la fecha de proceso
-- 2) calcula alojamiento consumos tours
-- 3) aplica descuentos según reglas de negocio
-- 4) convierte resultados a pesos chilenos
-- 5) inserta resultados en DETALLE_DIARIO_HUESPEDES

CREATE OR REPLACE PROCEDURE sp_proceso_cobranza (
   p_fecha_proceso DATE,
   p_valor_dolar   NUMBER
)
IS

   --------------------------------------------------
   -- cursor:
   -- se usa TRUNC para evitar problemas de hora
   -- se ordena por fecha salida y apellido paterno
   
   CURSOR c_huespedes IS
      SELECT h.id_huesped,
             h.nom_huesped || ' ' ||
             h.appat_huesped || ' ' ||
             h.apmat_huesped nombre,
             r.estadia,
             r.id_reserva
      FROM huesped h
      JOIN reserva r
        ON h.id_huesped = r.id_huesped
      WHERE TRUNC(r.ingreso + r.estadia)
            = TRUNC(p_fecha_proceso)
      ORDER BY TRUNC(r.ingreso + r.estadia),
         h.appat_huesped;

   -- variables en dólares
   v_agencia                VARCHAR2(100);
   v_alojamiento            NUMBER := 0;
   v_consumos               NUMBER := 0;
   v_tours                  NUMBER := 0;
   v_valor_persona          NUMBER := 0;
   v_subtotal               NUMBER := 0;
   v_descuento_consumos     NUMBER := 0;
   v_descuento_agencia      NUMBER := 0;
   v_total                  NUMBER := 0;
   v_pct_tramo              NUMBER := 0;

BEGIN

   --------------------------------------------------
   -- limpieza de tablas
   
   DELETE FROM detalle_diario_huespedes;
   DELETE FROM reg_errores;

   FOR rec IN c_huespedes LOOP

      --------------------------------------------------
      -- 1) agencia del huésped
      
      v_agencia := fn_agencia_huesped(rec.id_huesped);

      --------------------------------------------------
      -- 2) alojamiento
      -- (valor habitación + minibar) * días estadía
      -- todo en dólares
      
      v_alojamiento :=
         fn_calcular_alojamiento(rec.id_reserva, rec.estadia);

      --------------------------------------------------
      -- 3) consumos
      
      v_consumos := fn_monto_consumos(rec.id_huesped);

      --------------------------------------------------
      -- 4) tours
      
      v_tours := pkg_cobranza.fn_monto_tours(rec.id_huesped);

      --------------------------------------------------
      -- 5) valor fijo por persona
      -- se cobran $35.000 CLP por persona
      -- se convierte a dólares antes de sumar
      
      v_valor_persona := 35000 / p_valor_dolar;

      --------------------------------------------------
      -- 6) subtotal en dólares
      -- alojamiento + consumos + tours + valor persona
      
      v_subtotal :=
            v_alojamiento
          + v_consumos
          + v_valor_persona;

      --------------------------------------------------
      -- 7) descuento por tramo de consumo
      -- se consulta tabla TRAMOS_CONSUMOS
      
      BEGIN
         SELECT pct
         INTO v_pct_tramo
         FROM tramos_consumos
         WHERE v_consumos BETWEEN vmin_tramo AND vmax_tramo;

         v_descuento_consumos :=
            v_consumos * v_pct_tramo;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            v_descuento_consumos := 0;
      END;

      --------------------------------------------------
      -- 8) descuento adicional agencia
      -- solo aplica si es VIAJES ALBERTI (12%)
      
      v_descuento_agencia :=
         fn_descuento_agencia(
            v_agencia,
            (v_subtotal - v_descuento_consumos)
         );

      --------------------------------------------------
      -- 9) total final en dólares
      
      v_total :=
            v_subtotal
          - v_descuento_consumos
          - v_descuento_agencia;

      --------------------------------------------------
      -- 10) inserción en pesos chilenos
      -- todos los valores se redondean a enteros
      
      INSERT INTO detalle_diario_huespedes (
         id_huesped,
         nombre,
         agencia,
         alojamiento,
         consumos,
         tours,
         subtotal_pago,
         descuento_consumos,
         descuentos_agencia,
         total
      )
      VALUES (
         rec.id_huesped,
         rec.nombre,
         v_agencia,
         ROUND(v_alojamiento * p_valor_dolar),
         ROUND(v_consumos * p_valor_dolar),
         ROUND(v_tours * p_valor_dolar),
         ROUND(v_subtotal * p_valor_dolar),
         ROUND(v_descuento_consumos * p_valor_dolar),
         ROUND(v_descuento_agencia * p_valor_dolar),
         ROUND(v_total * p_valor_dolar)
      );

   END LOOP;

   COMMIT;

END;
/

------------------------------------------------------
-- EJECUCIÓN DEL PROCEDIMIENTO
-- se envía como parámetro
-- 1) fecha a procesar (18/08/2021)
-- 2) tipo de cambio dólar (915 CLP)
-- no se usan fechas fijas dentro del procedimiento solo se pasan como parámetro externo

BEGIN
   sp_proceso_cobranza(
      TO_DATE('18/08/2021','DD/MM/YYYY'),
      915
   );
END;
/

------------------------------------------------------
-- EJECUCIÓN CASO 1
-- se consulta TOTAL_CONSUMOS para validar que
-- el trigger actualizó correctamente los montos luego de insertar eliminar y actualizar consumos

SELECT id_consumo,
       id_reserva,
       id_huesped,
       monto
FROM consumo
WHERE id_huesped IN (340006, 340008)
ORDER BY id_huesped, id_consumo;

SELECT id_huesped,
       monto_consumos
FROM total_consumos
WHERE id_huesped IN (340003,340004,340006,340008,340009)
ORDER BY id_huesped;


------------------------------------------------------
-- EJECUCIÓN CASO 2
-- se consulta la tabla DETALLE_DIARIO_HUESPEDES
-- para revisar alojamiento, consumos, tours, subtotal, descuentos, total final
-- todos los valores almacenados en pesos chilenos

SELECT *
FROM detalle_diario_huespedes
ORDER BY id_huesped;


------------------------------------------------------
-- EJECUCIPON TABLA REG_ERRORES
-- permite validar que la función FN_AGENCIA_HUESPED registró correctamente huéspedes sin agencia asociada

SELECT *
FROM reg_errores
ORDER BY id_error;
