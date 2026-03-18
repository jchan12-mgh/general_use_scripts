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

if(bargs_in$add_log %in% 1){
  log_loc <- file.path(output_loc, glue("{name_of_report}.log"))
  cat(glue("-------- for run info check log file at {log_loc} ----- \n\n"))
  curr_commit <- system("git log --pretty=format:'%h' -n 1", intern=T)
  
  sf <- file(log_loc, open = "wt")
  sink(sf, split = T)
  sink(sf, type = "message")
  
  cat(glue("------------------- starting log file - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  cat(glue("------------------- Current git hash - {curr_commit} ------------------- \n\n"))
  
}


### Load data -----
cat(glue("------------------- Loading Data - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))

coh_params <- get_rc_params(token_pid59491)
ps_env_list <- get_env_list(glue("{dropbox_loc}/precise/ps"))

coll_dt_id_vrbs <- c("record_id", 
                     "redcap_event_name", 
                     "redcap_repeat_instrument", 
                     "redcap_repeat_instance")

ds_dd_ps <- ps_env_list$ds_dd_ps()
formps_list <- ps_env_list$formps_list()

query_optional_vrs <- ps_env_list$query_optional_vrs()
form_completeness <- ps_env_list$form_completeness()
rt_date <- ps_env_list$today()
ds_qy_ps <- ps_env_list$ds_qy_ps()
ds_em_ps <- ps_env_list$ds_em_ps()
ds_ei_ps <- ps_env_list$ds_ei_ps()
autodd_queries <- ps_env_list$autodd_queries()
queries_list <- ps_env_list$queries_list()

today_tm <- format(Sys.time(), "%Y%m%d_%H%M")
name_of_report <- "ps_dq"

# Create the output folder for these query-related datasets
q_fldr <- glue("../queries/{name_of_report}/{today_tm}")
dir.create(q_fldr, recursive = T)

log_loc <- file.path(q_fldr, "ped_mkrenv.log")
cat(glue("-------- for run info check log file at {log_loc} ----- \n\n"))
sf <- file(log_loc, open = "wt")
if(bargs_in$add_log == 1){
  sink(sf, split = T)
  sink(sf, type = "message")
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

## Missingness queries (variable, form, and visit -level) ---------

# extract only the coll_dt (collection date) info from all the form ds's, 
# and join into a single ds


# One row per record per event per form with collection date of form
# lambda function applied to each form in ds_dd
# first compares forms in ds_dd against formds_list
# parses various colldt variables including id_colldt, demo_colldt in each form
# selects any listed collection date and adds to a col coll_dt
# coerces character type to date for each form so we can bind rows of each form into full DS

##-------------- Missingness Queries - visit, form & variable level ---------
# modified form_completeness dataset, used for missingness
# Removing ABCD and MUSIC participants as denoted by Record numbers 99 and 53
# This is a full dataset pertaining to form completeness
fcomp_full_notimelimit <- form_completeness %>% 
  filter(!grepl("_fqueries$", variable)) %>% 
  filter(expected) %>% 
  left_join(ds_ei_ps, by = join_by(redcap_event_name==unique_event_name)) %>% 
  # filter(grepl('ya$', variable)) %>%
  filter(form %!in% forms_to_remove,
         variable %!in% vrs_to_remove) %>% 
  mutate(expected_n = sum(expected),
         prop_complete = prop_calc(sum(complete), sum(expected)), 
         missing_vrs = paste(variable[expected & !complete], collapse = ", "),
         .by = c(record_id, redcap_event_name, form)) %>% 
  # form is considered missed if less than 80% of expected questions are complete
  mutate(form_missed = prop_complete <= form_level_only_cutoff)  %>% 
  mutate(fullvisit_missed = length(unique(form[form_missed])) > 2,
         .by = c(record_id, redcap_event_name))


del_vr_query <- ds_qy_ps %>% 
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
  gen_link_ds(token_pid59491, ds_dd_ps, nm_vr = "field_name", nm_rec = "record", nm_instance="instance")

del_fm_query <- ds_qy_ps %>% 
  filter(grepl("Form expected. Can", comment, ignore.case = T),
         query_status == "OPEN",
         !is.na(res_id)) %>% 
  summarise(n_comments = n(), 
            .by=c(record, event_id, field_name, instance, status_id, res_id)) %>% 
  left_join(ds_dd_ps %>% 
              select(field_name, 
                     form = form_name),
            by = join_by(field_name)) %>% 
  left_join(fcomp_full_notimelimit %>% 
              select(record_id, event_id, form, prop_complete) %>% 
              distinct() %>% 
              mutate(curr_res = prop_complete > .9),
            by=join_by(record == record_id, event_id, form)) %>% 
  filter(curr_res %in% T | is.na(curr_res)) %>%
  gen_link_ds(token_pid59491, ds_dd_ps, nm_vr = "field_name", nm_rec = "record", nm_instance="instance")

del_vs_query <- ds_qy_ps %>% 
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
  gen_link_ds(token_pid59491, ds_dd_ps, nm_vr = "field_name", nm_rec = "record", nm_instance="instance")


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
  left_join(ds_dd_ps %>% 
              filter(grepl("_fqueries$", field_name)) %>% 
              select(variable = field_name, form = form_name),
            by = join_by(form)) %>% 
  mutate(fullvisit_missed = length(unique(form)) > 2,
         .by = c(record_id, redcap_event_name)) # To be removed shortly after checking on age requirements nov30

# Completeness of form - form-level query 
form_miss_queries <- fcomp_full_form_level %>% 
  # filter(!fullvisit_missed) %>% 
  mutate(comment = "Form expected. Can (additional) data be filled in?") %>% 
  select(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance, form, variable, comment)


# Completeness of visit form - visit-level query 
# visit_miss_queries <- fcomp_full_form_level %>% 
#   filter(fullvisit_missed) %>% 
#   select(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance) %>% 
#   distinct() %>% 
#   mutate(form = "visit_form",
#          variable = "visit_fqueries",
#          comment = "Visit Incomplete. Can (additional) data be filled in?")


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
#' temporary removals of handwritten queries. These should be fixed in the code that creates them. 
#' Note that any queries without a variable given cannot be pushed. these should be fixed. 


#########TODO take a look at this##########
# these are being queried elsewhere

# Auto-dd queries (range checks) ------

##########

autodd_queries <- tibble(bind_rows(autodd_queries)) %>% 
  select(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance,  
         query_valtxt, query, form, variable) %>% 
  filter(form %!in% aqforms_to_remove,
         variable %!in% aqvrs_to_remove,
         query %!in% aqqueries_to_remove)


# retrieve all existing REDCap queries
ds_qy_drc <- ds_qy_ps %>% 
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
  left_join(ds_dd_ps %>% 
              select(variable = field_name, form=form_name),
            by = join_by(variable)) %>% 
  mutate(across(redcap_repeat_instance, \(x) ifelse(is.na(x) | x %in% "", 1, as.numeric(x)))) %>% 
  left_join(ds_qy_drc,
            by = join_by(redcap_event_name, redcap_repeat_instance, record_id, variable)) %>% 
  left_join(ds_ei_ps, by = join_by(redcap_event_name==unique_event_name)) %>% 
  mutate(pid = coh_params$pid, 
         vr_url = gen_link(coh_params$rc_version, pid, redcap_repeat_instance, event_id, record_id, form, variable, url=coh_params$url)$link) %>% 
  filter(!is.na(record_id))

del_val_queries <- ds_qy_ps %>% 
  filter(!is.na(res_id),
         !grepl("_fqueries$", field_name),
         query_status == "OPEN") %>% 
  filter(!any(grepl("Data expected. Can data be filled in", comment)),
         .by=status_id) %>% 
  left_join(ds_dd_ps %>% 
              select(field_name, valmax = text_validation_max),
            by = join_by(field_name)) %>% 
  filter(valmax %!in% "today" & !grepl("today$", comment[1]),
         .by=status_id) %>%
  filter(status_id %!in% all_queries$status_id) %>%
  gen_link_ds(token_pid59491, ds_dd_ps, nm_vr = "field_name", nm_rec = "record", nm_instance = "instance")


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
    
    # Upload all of the new queries for the adult main cohort
    new_queries$push_result <- new_queries %>% push_query_coh(token_peds, coh_params$pid, coh_params$urlapi, ds_dd=ds_dd)
    
    all_dels <- dqpush_close(all_dels, token_peds, ds_dd, ds_qy)
    
    old_queries_toclose <- ds_qy %>%
      filter(query_status == "OPEN") %>%
      filter(status_id %!in% all_dels$status_id) %>% 
      filter(grepl("Data expected. Can data be filled in", comment) |
               grepl("_fqueries", field_name)) %>% 
      summarise(push_dt = min(as.Date(ts)),
                .by=c(status_id, field_name)) %>% 
      filter((Sys.Date() - push_dt) > 30) %>% 
      left_join(ds_dd %>% select(field_name = vr.name, form=form.name),
                by = join_by(field_name)) %>% 
      left_join(form_groupings,
                by = join_by(form)) %>% 
      filter(form_group == "participant surveys" | form %in% "visit_form") %>% 
      mutate(comment = "Lingering missingness queries are closed after 1 month. Data can still be filled in if available and contemporaneous.")
    
    old_queries_toclose <- dqpush_close(old_queries_toclose, token_peds, ds_dd, ds_qy)
    
    new_queries %>% 
      relocate(record_id) %>%  
      write.csv(glue("{q_fldr}/new_queries_adult_{today_tm}.csv"), row.names = F)
    
    all_queries %>% 
      relocate(record_id) %>% 
      filter(!is.na(query_status)) %>% 
      write.csv(glue("{q_fldr}/old_queries_adult_{today_tm}.csv"), row.names = F)
  }
} else {
  write.csv(new_queries, glue("{q_fldr}/queries_{rt_date}.csv"))
  
  ids <- unique(core$site)
  
  for(site_id in ids){
    new_queries %>%
      left_join(core %>% select(record_id, site), by = join_by(record_id)) %>% 
      filter(site == site_id) %>% 
      write.csv(glue("{q_fldr}/{site_id}_queries_{rt_date}.csv"))
  }
}
