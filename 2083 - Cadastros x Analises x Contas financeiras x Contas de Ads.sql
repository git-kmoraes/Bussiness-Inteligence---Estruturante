with periodo as ( 
--cria um intervalo com os ultimos 6 meses para facilitar a visualização do gráfico
	select periodo::date as inicio,  (date_trunc('month', periodo.periodo::date) + '1month'::interval - '1day'::interval)::date as fim
	from generate_series(date_trunc('month', now()::date)::date - '5 months'::interval,
                         date_trunc('month', now()::date)::date,
                         '1month'::interval) as periodo
)
, testes as (
-- lista todos os email que temos cadastrados e indica se cada email é de teste a55 ou não
	select distinct clp.email, SUBSTRING(clp.email FROM '.{9}$') ,
	case when SUBSTRING(clp.email FROM '.{9}$') = '@a55.tech' then true else false end as teste
	from crm.customer_login_pipeline clp
)
, step_dados_financeiros as (
-- lista todos os cnpjs que cadastraram a conta bancaria ou enviaram ofx
    select count(rbap.customer_id) as contas_bancarias_vinculadas, periodo.inicio
    from crm.all_customer_pipelines acp
    left join crm.register_bank_account_pipeline rbap on acp.account_pipeline_id = rbap.id
    left join crm.hubspot_pipeline   	              on hubspot_pipeline.id = acp.hubspot_pipeline_id 
    left join testes 						 		  on acp.email = testes.email
    left join periodo 					     		  on rbap.bank_account_registered_date::date between periodo.inicio::date and periodo.fim::date
    where rbap.bank_account_registered_date is not null
    	and not testes.teste
        [[and {{origination}}]]
        [[and {{origination_partner}}]]
    group by periodo.inicio
)
, step_extrato_populado as (
-- lista todos os cnpjs que alem de cadastrar a conta bancaria popularam o extrato
	select count(rbap.customer_id) as extrato_populado, periodo.inicio
	from crm.all_customer_pipelines acp
	left join crm.register_bank_account_pipeline   rbap on acp.account_pipeline_id = rbap.id 
	left join crm.hubspot_pipeline                      on hubspot_pipeline.id = acp.hubspot_pipeline_id
    left join testes 						 			on acp.email = testes.email
	left join periodo 						 			on rbap.polished_statement_table_date::date between periodo.inicio::date and periodo.fim::date
	where rbap.polished_statement_table_date is not null 
	and rbap.bank_account_registered_date is not null 
	  and not testes.teste
	  [[and {{origination}}]]
	  [[and {{origination_partner}}]]
	group by periodo.inicio
) 
, step_analises_realizadas as (
-- lista todos os clientes que passaram pela analise recebendo uma proposta
	select count(pp.customer_id) as analises_realizadas, periodo.inicio 
	from crm.all_customer_pipelines acp
	left join crm.proposal_pipeline 	 		 	pp	on acp.proposal_pipeline_id = pp.id
	left join crm.hubspot_pipeline                      on hubspot_pipeline.id = acp.hubspot_pipeline_id 
	left join crm.register_bank_account_pipeline   rbap on rbap.id = acp.account_pipeline_id 
    left join testes 						 	 		on acp.email = testes.email
	left join periodo 							        on pp.last_update::date between periodo.inicio::date and periodo.fim::date
	where rbap.bank_account_registered_date is not null -- clientes que conectaram a conta 
	  and rbap.polished_statement_table_date is not null -- clientes que popularam extrato
	  and (pp.state = 'proposal_accepted' or pp.state = 'proposal_approved') -- clientes que tiveram alguma proposta diferente da indicativa
	  and not testes.teste
	  [[and {{origination}}]]
	  [[and {{origination_partner}}]]
	group by periodo.inicio 
	order by periodo.inicio
)
, step_aceite_proposta as (
--lista todos os clientes que aceitaram uma proposta
	select count(pp.customer_id) propostas_aceitas, periodo.inicio 
	from crm.proposal_pipeline pp
	left join crm.all_customer_pipelines    acp on acp.proposal_pipeline_id = pp.id 
    left join crm.hubspot_pipeline           on hubspot_pipeline.id = acp.hubspot_pipeline_id
    left join testes 						 on acp.email = testes.email
	left join periodo 					     on pp.proposal_accepted_date::date between periodo.inicio::date and periodo.fim::date
	where pp.state = 'proposal_accepted'
	[[and {{origination}}]]
	[[and {{origination_partner}}]]
	group by periodo.inicio
)
, step_contas_cadastradas as (
	select 	count(acp.customer_id) cadastros, periodo.inicio
	from crm.all_customer_pipelines acp
	left join crm.customer_login_pipeline clp on acp.login_pipeline_id = clp.id
	left join crm.hubspot_pipeline            on acp.hubspot_pipeline_id = hubspot_pipeline.id
	left join testes 					      on testes.email = acp.email
	left join periodo 					      on clp.registered_date::date between periodo.inicio::date and periodo.fim::date  
	where not testes.teste
	  [[and {{origination}}]]
	  [[and {{origination_partner}}]]
	group by periodo.inicio
) 
select sf.inicio, sc.cadastros, sd.contas_bancarias_vinculadas, 
	   sf.extrato_populado, sa.analises_realizadas, sp.propostas_aceitas
from step_extrato_populado sf
left join step_contas_cadastradas sc on sc.inicio = sf.inicio
left join step_dados_financeiros sd on sf.inicio = sd.inicio
left join step_analises_realizadas sa on sa.inicio = sf.inicio 
left join step_aceite_proposta sp on sp.inicio = sf.inicio
where sf.inicio is not null
order by sf.inicio