top_dir <- system("git rev-parse --show-toplevel", intern = T, ignore.stderr = T)
if(length(top_dir) == 0) top_dir = "."
setwd(top_dir)


source("https://raw.githubusercontent.com/jchan12-mgh/general_use_scripts/refs/heads/main/helper_fxns.R")


# Required directory structure
# rt is the project folder
# rt has DM_src, DM, and codespace folders

proj_nm = "project_name"
enr_form_nm = "..."
## Survey queue and form display logic currently need to be downloaded manually and placed in higher level folder. 
## Name of files needed for search until API operational
sq_str <- "lCoho_SurveyQueue_"
fd_str <- "fdl_export_pid58_"


rt <- "path_to_root"

proj_loc <- file.path(rt, "DM_src", proj_nm)

bargs_mkrenv <- getArgs(defaults = list(dt = NA, pf=NA, log=0))

today <- format(Sys.Date(), "%Y%m%d")

cat(glue("------------------- project_name.R log file - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))

# This should work if you have the repository in the folder with your initials in the ws directory

max_fl_dt <- function(dir) max(as.numeric(list.files(proj_loc)), na.rm=T)

if(is.na(bargs_mkrenv$dt)){
  rt_date <- max_fl_dt(rc_sites_dir)
} else {
  rt_date <- bargs_mkrenv$dt
}

rt_date_dt <- ymd(rt_date)

if(is.na(bargs_mkrenv$pf)) {
  dir_pf = ""
} else {
  dir_pf = bargs_mkrenv$pf
}


dm_loc <- file.path(rt, "DM", proj_nm, paste0(rt_date, dir_pf))
dir.create(dm_loc, recursive = "T")
dm_src_loc <- file.path(proj_loc, rt_date)

if(bargs_mkrenv$log == 1) {
  log_loc <- file.path(dm_loc, "project_name.log")
  cat(glue("-------- for run info check log file at {log_loc} ----- \n\n"))
  sf <- file(log_loc, open = "wt")
  sink(sf, split = T)
  sink(sf, type = "message")
}


writeLines(glue("Source Data Date = {rt_date}"), file.path(dm_loc, glue("src_dt_{rt_date}")))


cat(glue("------------------- working with data from {rt} ------------------- \n\n"))

ds_dd <- get_folder_fxn(dm_src_loc, "_DataDictionary_", read=T)

ds_ua <- get_folder_fxn(dm_src_loc, "_userassigns_", read=T)
ds_dg <- get_folder_fxn(dm_src_loc, "_dagassigns_", read=T)
ds_em <- get_folder_fxn(dm_src_loc, "_eventmap_", read=T)
ds_ei <- get_folder_fxn(dm_src_loc, "_eventidmap_", read=T)

ds_sq <- tryCatch(get_folder_fxn(file.path(proj_loc, "../survey_queues"), sq_str, read=T), error=function(e) data.frame())
ds_fd <- tryCatch(get_folder_fxn(file.path(proj_loc, "../survey_queues"), fd_str, read=T), error=function(e) data.frame())

# keys for future joins
kys <- c("record_id", "redcap_event_name", "redcap_repeat_instrument", "redcap_repeat_instance")
#=========================== Global functions =================================
all_rfiles_loc <- file.path(dm_src_loc, "all_rfiles")
all_rfiles <- list.files(all_rfiles_loc)
all_rfile_stems <- unique(sub(".*DATA_(.+)_\\d{4}-\\d{2}-\\d{2}_\\d{2}_\\d{2}\\..*", "\\1", all_rfiles))

fdata_list <- nlapply(all_rfile_stems, \(x) form_read_fxn(x, all_rfiles, all_rfiles_loc, ds_dd))
formds_list <- lapply(fdata_list, "[[", "ds")
all_num_char_conv_issues <- bind_rows(lapply(fdata_list, "[[", "nc_issues"))
rm(fdata_list)

cat(glue("------------------- joining files - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
ds_fdata <- Reduce(function(x, y) {
  collapse::join(x %>% select(-any_of('form')), y %>% select(-any_of('form')), how="full", on = kys, overid=2)
}, formds_list)

ds_sq_r <- bind_rows(ds_sq %>% 
                       mutate(FD=0), 
                     ds_fd %>% 
                       rename(condition_logic=control_condition) %>% 
                       mutate(FD = 1)) %>% 
  get_sqrlogic()

afmts_list <- fmt_gen_fxn(ds_dd)

repeat_instruments <- repeat_instr_fxn(ds_fdata)

dd_list <- ddprep_fxn(ds_dd, repeat_instruments, cohort_nm = proj_nm)

query_optional_vrs <- c()

cat(glue("------------------- running expect_complete - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))

# ddprep work introduces shortcuts for REDCap logic that need to be accounted for in ds_fdata_full
#' enrollment form has all variables joined on with enr_vis_ prefix
#' full listing of these can be seen in the dd_prep function. Required additions can be seen in the failed logic from form_completeness generation

ds_fdata_full <- ds_fdata %>% 
  left_join(formds_list[[enr_form_nm]] %>% 
              rename_with(\(x) paste0("enr_vis_", x),
                          .cols=-all_of(kys)),
            by = join_by(record_id, redcap_event_name, redcap_repeat_instrument, redcap_repeat_instance))


form_completeness <- dd_list$dd_val %>% 
  filter(field_name %!in% c("record_id"),
         field_type %!in% c("calc"),
         !grepl("@HIDDEN|OLDCALC", field_annotation)) %>% 
  expect_complete_cfxn(ds = ds_fdata_full, cores_max = 5) %>%
  filter(variable %!in% query_optional_vrs) 

val_chks <- val_cfxn(dd_list$dd_val)

autodd_queries <- cq_fxn(val_chks, ds = ds_fdata_full, cores_max = 5, me=F)

rm(ds_fdata_full)

cat(glue("------------------- general queries complete - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))

print("Complete")

closeAllConnections()
