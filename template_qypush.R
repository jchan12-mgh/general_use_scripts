top_dir <- system("git rev-parse --show-toplevel", intern = T, ignore.stderr = T)
if(length(top_dir) == 0) top_dir = "."
setwd(top_dir)


source("https://raw.githubusercontent.com/jchan12-mgh/general_use_scripts/refs/heads/main/helper_fxns.R")

source(".tokens.R") # loaded if needed
source(".globalvars.R")

bargs_in <- getArgs(defaults = list(add_log = 0))

today <- format(Sys.Date(), "%Y%m%d")
today_tm <- format(Sys.time(), "%Y%m%d_%H%M")

## Start log if running from shell script ----

name_of_report <- "rep_name"
reports_out_fldr <- paste0("../reports/", name_of_report, "/")
output_loc <- paste0(reports_out_fldr, today_tm, "/")
dir.create(output_loc, recursive = T)

### Load data -----
cat(glue("------------------- Loading Data - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))

rsrc_list <- list()

rsrc_list$token <- token_pid59491
study_pf <- "ps"

coh_params <- get_rc_params(rsrc_list$token)
ps_env_list <- get_env_list(glue("{dropbox_loc}/PRECISE DCC/precise_data"))

coll_dt_id_vrbs <- c("record_id", 
                     "redcap_event_name", 
                     "redcap_repeat_instrument", 
                     "redcap_repeat_instance")


formps_list <- ps_env_list$formps_list()

query_optional_vrs <- ps_env_list$query_optional_vrs()
form_completeness <- ps_env_list$form_completeness()
rt_date <- ps_env_list$today()
rsrc_list$ds_qy <- ps_env_list[[paste("ds_qy", study_pf, sep="_")]]()
rsrc_list$ds_em <- ps_env_list[[paste("ds_em", study_pf, sep="_")]]()
rsrc_list$ds_ei <- ps_env_list[[paste("ds_ei", study_pf, sep="_")]]()
rsrc_list$ds_dd <- ps_env_list[[paste("ds_dd", study_pf, sep="_")]]()
autodd_queries <- ps_env_list$autodd_queries()
queries_list <- tryCatch(ps_env_list$queries_list(), error=function(e) list())

log_loc <- file.path(glue("{output_loc}/{name_of_report}.log"))
cat(glue("-------- for run info check log file at {log_loc} ----- \n\n"))
sf <- file(log_loc, open = "wt")
if(bargs_in$add_log %in% 1){
  cat(glue("-------- for run info check log file at {log_loc} ----- \n\n"))
  curr_commit <- system("git log --pretty=format:'%h' -n 1", intern=T)
  
  sink(sf, split = T)
  sink(sf, type = "message")
  
  cat(glue("------------------- starting log file - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  cat(glue("------------------- Current git hash - {curr_commit} ------------------- \n\n"))
}


###-----------Manage Variable and Forms here-----------


# Remove whole form from querying
forms_to_remove <- c()

# Use this to remove vars from handwritten queries
hand_written_query_remove_vrs <- NA

# Use this to remove vars coming up in auto queries
# Any contact info is removed from autoqueries because these are not required fields
vrs_to_remove <- c("cons_approachmult")

form_groups_to_remove <- c()

no_form_level_queries <- c()


aqforms_to_remove <- c()
aqvrs_to_remove <- c()
aqform_groups_to_remove <- c("")
aqqueries_to_remove <- c("")


# Use this to specify proportion of completeness that is acceptable.
# This is computed with prop_calc function
form_level_only_cutoff <- 0.8

## Specify form check limits here (days)
participant_survey_min <- 2
participant_survey_max <- 7
in_person_min <- 4
in_person_max <- 14
default_min <- 4
default_max <- 30
participant_survey_offset <- 21
default_offset <- 30

## Script helper functions ---------------------

prop_calc <- function(x, y){
  if(y == 0) return(NA)
  x/y
}

## API Setup ----------------

##----------Datasets----------------


fcomp_full_notimelimit <- form_completeness %>% 
  filter(!grepl("_fqueries$", variable)) %>% # why is this being removed?
  filter(expected) %>% 
  left_join(rsrc_list$ds_ei, 
            by = join_by(redcap_event_name==unique_event_name)) %>% 
  filter(form %!in% forms_to_remove,
         variable %!in% vrs_to_remove) %>% 
  mutate(expected_n = sum(expected),
         prop_complete = prop_calc(sum(complete), sum(expected)), 
         missing_vrs = paste(variable[expected & !complete], collapse = ", "),
         .by = c(record_id, redcap_event_name, form)) %>% 
  # form is considered missed if less than x% of expected questions are complete
  mutate(form_missed = prop_complete <= form_level_only_cutoff)  %>% 
  mutate(fullvisit_missed = length(unique(form[form_missed])) > 2,
         .by = c(record_id, redcap_event_name))


del_vr_query <- rsrc_list$ds_qy %>% 
  filter(grepl("Can data be filled in", comment, ignore.case = T),
         query_status == "OPEN",
         !is.na(res_id)) %>% 
  summarise(n_comments = n(), 
            .by=c(record, event_id, field_name, instance, status_id, res_id)) %>% 
  left_join(fcomp_full_notimelimit %>% 
              filter(!complete) %>% 
              mutate(instance = as.numeric(replace_na(redcap_repeat_instance, "1")),
                     curr_issue = T),
            by=join_by(record == record_id, event_id, 
                       field_name == variable, instance == instance)) %>% 
  filter(curr_issue %!in% T | is.na(curr_issue)) %>%
  gen_link_ds(rsrc_list$token, rsrc_list$ds_dd, nm_vr = "field_name", nm_rec = "record", nm_instance="instance")

del_fm_query <- rsrc_list$ds_qy %>% 
  filter(grepl("Form expected. Can", comment, ignore.case = T),
         query_status == "OPEN",
         !is.na(res_id)) %>% 
  summarise(n_comments = n(), 
            .by=c(record, event_id, field_name, instance, status_id, res_id)) %>% 
  left_join(rsrc_list$ds_dd %>% 
              select(field_name, 
                     form = form_name),
            by = join_by(field_name)) %>% 
  left_join(fcomp_full_notimelimit %>% 
              select(record_id, event_id, form, prop_complete) %>% 
              distinct() %>% 
              mutate(curr_res = prop_complete > .9),
            by=join_by(record == record_id, event_id, form)) %>% 
  filter(curr_res %in% T | is.na(curr_res)) %>%
  gen_link_ds(rsrc_list$token, rsrc_list$ds_dd, nm_vr = "field_name", nm_rec = "record", nm_instance="instance")

del_vs_query <- rsrc_list$ds_qy %>% 
  filter(grepl("Visit Incomplete. Can", comment, ignore.case = T),
         query_status == "OPEN",
         !is.na(res_id)) %>% 
  summarise(n_comments = n(), 
            .by=c(status_id, record, field_name, event_id)) %>% 
  left_join(fcomp_full_notimelimit %>% 
              filter(fullvisit_missed) %>% 
              select(record_id, event_id) %>% 
              distinct() %>% 
              mutate(curr_issue=T),
            by=join_by(record == record_id, event_id)) %>% 
  filter(curr_issue %!in% T | is.na(curr_issue)) %>%
  gen_link_ds(rsrc_list$token, rsrc_list$ds_dd, nm_vr = "field_name", nm_rec = "record", nm_instance="instance")


fcomp_full <- fcomp_full_notimelimit 
# %>% 
  # filter(in_query_window)

# Missing variable 
variable_miss_queries <- fcomp_full %>% 
  filter(!form_missed, 
         !complete) %>% 
  mutate(comment = "Data expected. Can data be filled in?") %>% 
  select(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance, form, variable, comment)

# Missing form general -- this is used for missing form and missing visits below
fcomp_full_form_level <- fcomp_full %>% 
  filter(form %!in% no_form_level_queries,
         form_missed) %>% 
  select(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance, form) %>% 
  distinct() %>% 
  left_join(rsrc_list$ds_dd %>% 
              filter(grepl("_fqueries$", field_name)) %>% 
              select(variable = field_name, form = form_name),
            by = join_by(form)) %>% 
  mutate(fullvisit_missed = length(unique(form)) > 2,
         .by = c(record_id, redcap_event_name)) # To be removed shortly after checking on age requirements nov30

# Completeness of form - form-level query 
form_miss_queries <- fcomp_full_form_level %>% 
  filter(!fullvisit_missed) %>% 
  mutate(comment = "Form expected. Can (additional) data be filled in?") %>% 
  select(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance, form, variable, comment)

visit_miss_queries <- fcomp_full_form_level %>% 
  filter(fullvisit_missed) %>% 
  select(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance, form) %>% 
  mutate(comment = "Visit expected. Can (additional) forms be filled in?",
         variable = "some_visit_level_form")

# Handwritten queries (from mkforms, mkrenv) ------
# These are custom queries designed to check something specific 
hand_written_queries <- if(length(queries_list) == 0){
  qy_vrs <- c('record_id', 'redcap_event_name', 'redcap_repeat_instrument', 'redcap_repeat_instance', 
              'query_valtxt', 'query', 'form', 'variable')
  mk_ds_blank(qy_vrs)
} else {
  lapply(queries_list, function(ds) {
    ds$variable <- as.character(ds$variable)
  }) %>% bind_rows() %>% 
    select(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance, 
           query_valtxt, query, form, variable)
} %>% 
  filter(variable %!in% hand_written_query_remove_vrs)


autodd_queries <- tibble(bind_rows(autodd_queries)) %>% 
  select(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance,  
         query_valtxt, query, form, variable) %>% 
  filter(form %!in% aqforms_to_remove,
         variable %!in% aqvrs_to_remove,
         query %!in% aqqueries_to_remove)


# retrieve all existing REDCap queries
ds_qy_drc <- rsrc_list$ds_qy %>% 
  group_by(status_id, query_status, record_id = record, redcap_event_name, redcap_repeat_instance = instance, record_id, variable = field_name) %>% 
  summarise(queried_tf = T, 
            curr_comments = paste(glue("[{1:n()}] {comment}"), collapse = " | "),
            closed = any(current_query_status %in% "CLOSED"),
            open_responded = !closed & (n() > 1),
            open_noresponse = !closed & (n() == 1),
            .groups = "drop")

# TODO removed withdraw_DT checking on whether this is ok
# combine all three sources of queries into one ds, include a RC url link
all_queries <- autodd_queries %>%  
  mutate(across(where(is.factor), as.character)) %>% 
  mutate(across(c(query, query_valtxt), as.character)) %>% 
  bind_rows(hand_written_queries) %>% 
  summarise(comment = paste(gsub("\\\\", "", query_valtxt), collapse = ", "), 
            .by=c(redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance, record_id, variable)) %>% 
  bind_rows(variable_miss_queries, 
            form_miss_queries %>% 
              filter(variable %in% c("visit_fqueries")), 
            visit_miss_queries) %>% 
  select(-any_of("form")) %>% 
  left_join(rsrc_list$ds_dd %>% 
              select(variable = field_name, form=form_name),
            by = join_by(variable)) %>% 
  mutate(across(redcap_repeat_instance, \(x) ifelse(is.na(x) | x %in% "", 1, as.numeric(x)))) %>% 
  left_join(ds_qy_drc,
            by = join_by(redcap_event_name, redcap_repeat_instance, record_id, variable)) %>% 
  left_join(rsrc_list$ds_ei, by = join_by(redcap_event_name==unique_event_name)) %>% 
  mutate(pid = coh_params$pid, 
         vr_url = gen_link(coh_params$rc_version, pid, redcap_repeat_instance, event_id, record_id, form, variable, url=coh_params$url)$link) %>% 
  filter(!is.na(record_id))

del_val_queries <- rsrc_list$ds_qy %>% 
  filter(!is.na(res_id),
         !grepl("_fqueries$", field_name),
         query_status == "OPEN") %>% 
  filter(!any(grepl("Data expected. Can data be filled in", comment)),
         .by=status_id) %>% 
  left_join(rsrc_list$ds_dd %>% 
              select(field_name, valmax = text_validation_max),
            by = join_by(field_name)) %>% 
  filter(valmax %!in% "today" & !grepl("today$", comment[1]),
         .by=status_id) %>%
  filter(status_id %!in% all_queries$status_id) %>%
  gen_link_ds(rsrc_list$token, rsrc_list$ds_dd, nm_vr = "field_name", nm_rec = "record", nm_instance = "instance")


all_dels <- del_vr_query %>% select(status_id, url_out) %>% 
  bind_rows(del_fm_query %>% select(status_id, url_out)) %>% 
  bind_rows(del_vs_query %>% select(status_id, url_out)) %>% 
  bind_rows(del_val_queries %>% select(status_id, url_out)) %>% 
  mutate(comment= "Query resolved in data, addressed elsewhere, or removed from query list. Closed by DRC.") %>% 
  distinct() 

# final list of queries to push up into RC
new_queries <- all_queries %>% 
  filter(!queried_tf | is.na(queried_tf))

api_push = F
if(api_push) {
  queries_legit_yn =F # by default set to F, manually check/eye-ball the new queries
  if (queries_legit_yn) {
    new_queries <- new_queries %>% filter(grepl("test", record_id, ignore.case=T))
    # Upload all of the new queries for the adult main cohort
    new_queries$push_result <- new_queries %>% push_query_coh(rsrc_list$token, coh_params$pid, coh_params$urlapi, ds_dd=rsrc_list$ds_dd)
    
    failed_pushes <- new_queries %>% 
      filter(push_result == "[]")
    if(nrow(failed_pushes) > 0) print("There are some failed pushes that should be checked")
    
    
    all_dels <- dqpush_close(all_dels, rsrc_list$token, rsrc_list$ds_dd, rsrc_list$ds_qy)
    
    old_queries_toclose <- rsrc_list$ds_qy %>%
      filter(query_status == "OPEN") %>%
      filter(status_id %!in% all_dels$status_id) %>% 
      filter(grepl("Data expected. Can data be filled in", comment) |
               grepl("_fqueries", field_name)) %>% 
      summarise(push_dt = min(as.Date(ts)),
                .by=c(status_id, field_name)) %>% 
      filter((Sys.Date() - push_dt) > 30) %>% 
      left_join(rsrc_list$ds_dd %>% 
                  select(field_name, form=form_name),
                by = join_by(field_name)) %>% 
      filter(form %in% "visit_form") %>% # There may be relevant things to leave out of this
      mutate(comment = "Lingering missingness queries are closed after 1 month. Data can still be filled in if available and contemporaneous.")
    
    # old_queries_toclose <- dqpush_close(old_queries_toclose, rsrc_list$token, rsrc_list$ds_dd, rsrc_list$ds_qy)
    
    new_queries %>% 
      relocate(record_id) %>%  
      write.csv(glue("{output_loc}/new_queries_adult_{today_tm}.csv"), row.names = F)
    
    all_queries %>% 
      relocate(record_id) %>% 
      filter(!is.na(query_status)) %>% 
      write.csv(glue("{output_loc}/old_queries_adult_{today_tm}.csv"), row.names = F)
  }
} else {
  write.csv(new_queries, glue("{output_loc}/queries_{rt_date}.csv"))
  
  ids <- unique(core$site)
  
  for(site_id in ids){
    new_queries %>%
      left_join(core %>% select(record_id, site), by = join_by(record_id)) %>% 
      filter(site == site_id) %>% 
      write.csv(glue("{output_loc}/{site_id}_queries_{rt_date}.csv"))
  }
}
