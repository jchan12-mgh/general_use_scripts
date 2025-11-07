cat("\n\n------------------------------------------------",
    "\n------------------------------------------------",
    "\nStarting dm_proc.R run ",
    "\n------------------------------------------------ \n\n\n")


top_dir <- system("git rev-parse --show-toplevel", intern = T, ignore.stderr = T)
if(length(top_dir) == 0) top_dir = "."
setwd(top_dir)


source("https://raw.githubusercontent.com/jchan12-mgh/general_use_scripts/refs/heads/main/helper_fxns.R")

source(".globalvars.R")

bargs_mkrenv <- getArgs(defaults = list(dt = NA, pf=NA, log=0))

src_locs <- c(sg = glue("{dropbox_loc}/project_name/sig"),
              c1 = glue("{dropbox_loc}/project_name/csrp_cons"))

today <- format(Sys.Date(), "%Y%m%d")

if(is.na(bargs_mkrenv$pf)) {
  dir_pf = ""
} else {
  dir_pf = bargs_mkrenv$pf
}

rc_name1 <- sub(".*/([^/]+)/*$", "\\1", src_locs[1])

## Survey queue and form display logic currently need to be downloaded manually and placed in higher level folder. 
## Name of files needed for search until API operational

dm_src_loc1 <- file.path(src_locs[1], "DM_src")

# This should work if you have the repository in the folder with your initials in the ws directory

max_fl_dt <- function(dir) max(as.numeric(list.files(dir)), na.rm=T)

if(is.na(bargs_mkrenv$dt)){
  project_location_date <- max_fl_dt(dm_src_loc1)
} else {
  project_location_date <- bargs_mkrenv$dt
}

project_location_date_dt <- ymd(project_location_date)

dm_loc <- file.path(src_locs[1], "DM", paste0(project_location_date, dir_pf))
dir.create(dm_loc, recursive = "T")

writeLines(glue("Source Data Date = {project_location_date}"), file.path(dm_loc, glue("src_dt_{project_location_date}")))

# keys for future joins
kys <- c("record_id", "redcap_event_name", "redcap_repeat_instrument", "redcap_repeat_instance")


if(bargs_mkrenv$log == 1) {
  log_loc <- glue("{dm_loc}/{rc_name1}.log")
  cat(glue("-------- for run info check log file at {log_loc} ----- \n\n"))
  sf <- file(log_loc, open = "wt")
  sink(sf, split = T)
  sink(sf, type = "message")
}


proc_src <- function(loc, two_char_desc=""){

  out_list <- list()
  
  out_list$rc_namexx =  sub(".*/([^/]+)/*$", "\\1", loc)
  
  out_list$dm_src_locxx <- file.path(loc, "DM_src", project_location_date)
  
  cat(glue("------------------- working with data from {loc} ------------------- \n\n"))
  
  out_list$ds_ddxx <- get_folder_fxn(out_list$dm_src_locxx, "_DataDictionary_", read=T)
  
  out_list$ds_uaxx <- get_folder_fxn(out_list$dm_src_locxx, "_userassigns_", read=T)
  out_list$ds_dgxx <- get_folder_fxn(out_list$dm_src_locxx, "_dagassigns_", read=T)
  out_list$ds_emxx <- get_folder_fxn(out_list$dm_src_locxx, "_eventmap_", read=T)
  out_list$ds_eixx <- get_folder_fxn(out_list$dm_src_locxx, "_eventidmap_", read=T)
  out_list$ds_sqxx <- get_folder_fxn(out_list$dm_src_locxx, "_surveyqueue_", read=T)
  out_list$ds_fdxx <- get_folder_fxn(out_list$dm_src_locxx, "_formdisplaylogic_", read=T)
  #=========================== Global functions =================================
  all_rfiles_loc <- file.path(out_list$dm_src_locxx, "all_rfiles")
  all_rfiles <- list.files(all_rfiles_loc)
  all_rfile_stems <- unique(sub(".*DATA_(.+)_\\d{4}-\\d{2}-\\d{2}_\\d{2}_\\d{2}\\..*", "\\1", all_rfiles))
  
  fdata_list <- nlapply(all_rfile_stems, \(x) form_read_fxn(x, all_rfiles, all_rfiles_loc, out_list$ds_ddxx))
  # formds_list functions will break if this is named anything other than formds_list
  out_list$formzz_list <- lapply(fdata_list, "[[", "ds")
  out_list$all_num_char_conv_issuesxx <- bind_rows(lapply(fdata_list, "[[", "nc_issues"))
  
  cat(glue("------------------- joining files - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
 
  out_list$ds_fdataxx <- Reduce(function(x, y) {
    
    collapse::join(x %>% select(-any_of('form')), y %>% select(-any_of('form')), how="full", on = intersect(intersect(names(x), names(y)), kys), overid=2)
  }, out_list$formzz_list)
  
  out_list$ds_sq_rxx <- bind_rows(out_list$ds_sqxx %>% 
                         mutate(FD=0), 
                       out_list$ds_fdxx %>% 
                         rename(condition_logic=control_condition) %>% 
                         mutate(FD = 1)) %>% 
    get_sqrlogic()
  
  afmts_list <- fmt_gen_fxn(out_list$ds_ddxx)
  
  repeat_instruments <- if("redcap_repeat_instrument" %in% names(out_list$ds_fdataxx)){
    repeat_instr_fxn(out_list$ds_fdataxx)
  } else {
    data.frame(redcap_repeat_instrument= as.character(), repeat_instr= as.character())
  }
  
  out_list$dd_listxx <- ddprep_fxn(out_list$ds_ddxx, repeat_instruments, cohort_nm = out_list$rc_namexx, 
                                   sq_r = out_list$ds_sq_rxx, in_em = out_list$ds_emxx)
  
  names(out_list) <- gsub("xx", paste0("_", two_char_desc), names(out_list))
  if(two_char_desc == "") {
    names(out_list) <- gsub("zz", "ds", names(out_list))
  } else {
    names(out_list) <- gsub("zz", two_char_desc, names(out_list))
  }
  
  cat(glue("------------------- returning rc objects - {two_char_desc} - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  out_list
}

proc_status <- lapply(1:length(src_locs), \(i) {
  rc_list <- proc_src(src_locs[i], names(src_locs)[i])
  list2env(rc_list, envir = .GlobalEnv)
  data.frame(rc=names(src_locs)[i], objs = length(rc_list))
}) %>% bind_rows()

query_optional_vrs_rc <- c()

cat(glue("------------------- running expect_complete - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))

# ddprep work introduces shoproject_locationcuts for REDCap logic that need to be accounted for in ds_fdata_full
#' enrollment form has all variables joined on with enr_vis_ prefix
#' full listing of these can be seen in the dd_prep function. Required additions can be seen in the failed logic from form_completeness generation

# If query reports are needed across projects the code below would need to be duplicated


add_sing_vis_branching <- function(ds, ...){
  reduce(c(list(ds=ds), list(...)), \(x, y) {
    vis_info <- unique(y$redcap_event_name)
    if(length(vis_info) == 0) vis_info = "randomization_arm_1"
    if("redcap_repeat_instrument" %!in% names(y)) y$redcap_repeat_instrument = NA
    if(length(vis_info) > 1 | any(!is.na(y$redcap_repeat_instrument))) stop("This function can only add single visit forms. 
                                  If you want a specific visit for a form filter prior to adding as a parameter")
    left_join(x, y %>% 
                select(-matches("_complete$"), -any_of("form"),
                       -any_of(c("redcap_repeat_instrument", "redcap_repeat_instance", "redcap_event_name"))) %>% 
                rename_with(\(x) paste0(vis_info, x),
                            .cols=-record_id),
              by = join_by(record_id))
  })
}

ds_fdata_full <- dd_list_sg %>% 
  add_sing_vis_branching(formsg_list$screening,
                         formsg_list$consent, 
                         formsg_list$randomization_eligibility_confirmation,
                         formsg_list$randomization,
                         formsg_list$participant_demographics)


form_completeness <- dd_list_sg$dd_val %>% 
  filter(field_name %!in% c("record_id"),
         field_type %!in% c("calc"),
         !grepl("@HIDDEN|OLDCALC", field_annotation)) %>% 
  expect_complete_cfxn(ds = ds_fdata_full, cores_max = 5) %>%
  filter(variable %!in% query_optional_vrs_rc) 

val_chks <- val_cfxn(dd_list_sg$dd_val, project_location_date_dt)

autodd_queries <- cq_fxn(val_chks, ds = ds_fdata_full, cores_max = 5, me=F)

rm(ds_fdata_full)

cat(glue("------------------- general queries complete - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))


cat(glue("------------------- saving qs2 files - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))


save_info <- lapply(ls(), \(fl_str){
  if(grepl("^token_", fl_str)) {
    print(glue("{fl_str} - Do not save out your private token"))
    return(NA)
  }
  print(glue('Saving file {fl_str} to qs2'))
  obj_save <- eval(parse(text=paste0("`", fl_str, "`")))
  
  if(grepl("^form.._list$", fl_str)) {
    all_forms <- names(obj_save)
    lapply(all_forms, function(fm){
      qs_save(obj_save[[fm]], file.path(dm_loc, paste0(fl_str, "_", fm, "_rdsfxnobjhlpr", ".qs2")))
    })
  } else {
    qs_save(obj_save, file.path(dm_loc, paste0(fl_str, ".qs2")))
  }
  print(glue('Complete {fl_str} qs2'))
})

cat(glue("------- COMPLETE - {format(Sys.time(), '%H:%M')} ---------\n\n"))

closeAllConnections()
