/**********************************************************************************/
-- Esse teste compara a execução serial e paralela de uma rotina que lança débitos
/**********************************************************************************/

/* Script de limpeza do teste */

drop table debitos;
create table debitos(idimovel number, valdeb number);

begin
    for i in 1..500000 loop
        insert into debitos values(i,i*2.5);
    end loop;
    commit;
end;
/

drop table lancamento;
create table lancamento(idimovel number, ano number, vallanc number);

create or replace procedure stp_lancamento(p_ano number, p_terminacao number default null) is
cursor c1 is
       select idimovel,valdeb
       from debitos
       where p_terminacao is null
       or 
       p_terminacao is not null and substr(idimovel,-1)=p_terminacao;
v_ini date;       
begin
     v_ini:=sysdate;
     for r1 in c1 loop
         insert into lancamento(idimovel,ano,vallanc)
         values(r1.idimovel,p_ano,r1.valdeb);
     end loop;
     insert into log(ano,terminacao,dataini,datafim)
     values(p_ano,p_terminacao,v_ini,sysdate);
     commit;
end;
-- Fim do Script

-- Teste Serial - executar antes script de limpeza
begin
      delete from log where terminacao is null;
      stp_lancamento(2015);
      commit;
end;
/
select count(0) from lancamento;
select * from log;

-- Teste Paralelo - executar antes script de limpeza
declare
job_num number;
grau_paralelismo number;
begin
      delete from log where terminacao is not null;

      grau_paralelismo:=10;
      for terminacao in 0..grau_paralelismo-1 loop
          dbms_job.submit(job=> job_num,
                          what => 'begin stp_lancamento(2015,'||terminacao||'); end;');
      end loop;
      commit;
end;

select count(0) from lancamento;
select * from log;

