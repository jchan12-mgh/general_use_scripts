#' templates needed
#' API call to save raw data from REDCap. will include all supplemental datasets available
#' processing code (i.e. mkrenv)
#' reporting/analysis template
#' 
#' 

if(length(system("git rev-parse --show-toplevel", intern = T, ignore.stderr = T)) == 0){
  print("Not currently in a repo. Continuing in current wd")
} else {
  if(getwd() != system("git rev-parse --show-toplevel", intern = T, ignore.stderr = T)) print("To run this script properly you need to be at the top level of the repo")
}


curr_fxns <- as.vector(lsf.str())


## General suite of packages we use. Not 100% necessary but shouldn't be any drawbacks from leaving in the large list
## don't load plyr
pckgs <- c(
  "Matching"  , "DiagrammeR" , "gapminder"    , "ggvenn"     , "tidytext"  , 
  "emayili"   , "gtsummary"  , "rlang"        , "MatchIt"    , "cobalt"    , 
  "patchwork" , "corrplot"   , "RColorBrewer" , "factoextra" , "cluster"   ,
  "httr"      , "haven"      , "glue"         , "scales"     , "qs2"       ,
  "stringi"   , "flextable"  , "magrittr"     , "openxlsx"   , "collapse"  ,
  "officer"   , "questionr"  , "data.table"   , "htmltools"  , "vroom"     ,
  "janitor"   , "pagedown"   , "here"         , "readxl"     , "docstring" ,
  "jsonlite"  , "tools"      , "rmarkdown"    , "conflicted" , "stopwords" ,
  "cowplot"   , "knitr"      , "zip"          , "ftExtra"    , "xml2"      ,
  "tidyverse" , "crosstable" , "tibble"
)

load_libs <- function(add_libs , install_pcks=T){
  
  loaded_pcks <- .packages()
  
  loaded_note <- intersect(add_libs, loaded_pcks)
  new_pcks <- setdiff(add_libs, loaded_pcks)
  if(length(loaded_note) > 0) print(paste("Packages already loaded:", paste(loaded_note, collapse=", ")))
  if(length(new_pcks) == 0) return("All packages already loaded")
  if(install_pcks) {
    sapply(new_pcks, function(x) {
      loaded = suppressWarnings(do.call("require", list(x)))
      if(!loaded){
        install.packages(x, repos = 'http://cran.us.r-project.org')
        loaded = suppressWarnings(do.call("require", list(x)))
        if(loaded) {
          print(paste(x, " installed and loaded"))
        } else {
          print(paste(x, " could not be installed"))
        }
      }
    })
  } else {
    print("Loading...")
    sapply(new_pcks, function(x) suppressWarnings(do.call("require", list(x))))
  }
  
}

load_libs(pckgs)

## since the conflicts package is loaded these preferences are generally useful

conflicts_prefer(dplyr::filter())
conflicts_prefer(dplyr::select())
conflicts_prefer(dplyr::lag())
conflicts_prefer(dplyr::first())
conflicts_prefer(dplyr::last())
conflicts_prefer(dplyr::summarize())
conflicts_prefer(rmarkdown::render())
conflicts_prefer(lubridate::month)
conflicts_prefer(lubridate::year)
conflicts_prefer(lubridate::day)
conflicts_prefer(flextable::as_flextable)
conflicts_prefer(base::cbind())
conflicts_prefer(rlang::`:=`)
conflicts_prefer(zip::unzip)
conflicts_prefer(flextable::border)
conflicts_prefer(flextable::font)
conflicts_prefer(tidyr::replace_na)
conflicts_prefer(vroom::cols)
conflicts_prefer(vroom::col_character)

# lapply that will return a _named_ list

nlapply <- function(x, ...) setNames(lapply(x, ...), x)

nilapply <- function(x, ...) setNames(lapply(1:length(x), ...), x)

# function for opening scripts from the repo

flre_list <- nlapply(list.files(recursive = T), function(fl){
  fxn_out <- eval(parse(text = paste0("function() {
    curr_dir <- getwd()
    if(!in_top_dir) warning(glue(\"Your current directory is {curr_dir} but needs to be the top level of the repo for this function to work ({top_dir})\"))
    if('", fl, "' %in% list.files(recursive = T)) {file.edit('", fl, "')} else {warning('", fl, ", does not exist in your current working directory')}
  }")))
})

# function to search for files and open if needed

flre_search <- function(str, open, open_all=F){
  fls <- grep(str, names(flre_list), value=T, ignore.case = T)
  
  if(!missing(open)){
    for(i in open) flre_list[[fls[i]]]()
  } else if(open_all){
    for(fl in fls) flre_list[[fl]]()
  } else {
    return(fls)
  }
}

## convenience all to qs_read that limits to a single thread

qs_read1 <- function(ds, ...){
  qs_read(ds, nthreads=qopt("nthreads", 1), ...)
}

## loads saved processed files references into a named list of functions that will load data when called

get_env_list <- function(proj_loc, dt){
  proj_dm_loc <- file.path(proj_loc, "DM")
  
  if(missing(dt)) dt <- suppressWarnings(max(as.numeric(list.files(proj_dm_loc)), na.rm=T))
  print(glue("loading data from {dt}"))
  env_loc <- file.path(proj_dm_loc, dt)
  
  all_rds <- list.files(env_loc, pattern="\\.rds$|\\.qs2")
  
  out_list <- lapply(all_rds, function(fl){
    read_fxn <- ifelse(grepl("rds$", fl), "read_rds", "qs_read1")
    return(eval(parse(text = paste0("function() {tm1 = Sys.time(); outds <- ",
                                    read_fxn, 
                                    "('", 
                                    file.path(env_loc, fl), 
                                    "'); tm2 = Sys.time(); message(paste0('Load time: ', format(round(tm2 - tm1, 3), nsmall = 3))); return(outds)}"))))
  })
  
  names(out_list) <- gsub("\\.rds$|\\.qs2$", "", all_rds)
  fds_lists_srch <- grep("_rdsfxnobjhlpr", all_rds, value=T)
  fds_lists <- unique(gsub("(^form.._list)_.+", "\\1", fds_lists_srch))
  if(length(fds_lists_srch) == 0){
    warning("no formds_list files were found")
    warning(glue("env_list has {length(out_list)} elements"))
  } else {
    for(fds_list in fds_lists){
      rds_fdsl_fxngen <- function() {
        local_dir <- env_loc
        
        fds_ds <- data.frame(full = list.files(local_dir, pattern = paste0(fds_list, "_.+_rdsfxnobjhlpr\\..+"))) %>% 
          mutate(obj = gsub("^form.._list_|_rdsfxnobjhlpr.+", "", full)) %>% 
          mutate(type = ifelse(grepl("\\.rds$", full), "rds", "qs2"))
        
        if(any(duplicated(fds_ds$full))) stop("Why are there duplicated form names in REDCap?")
        
        fxn_block <- "
        fms <- unlist(as.list(environment()))
        if(any(fms == 'TRUE')) {
          forms <- unique(c(..., names(fms[fms == 'TRUE'])))
        } else {
          forms <- unique(c(...))
        }
        
        tm1 = Sys.time()
        print(paste('Reading from ', local_dir))
        
        
        outfds_list <- if(length(forms) == 0){
          setNames(lapply(1:nrow(fds_ds), \\(i) {
            
            read_fxn <- ifelse(fds_ds$type[i] == 'rds', read_rds, qs_read1)
            read_fxn(file.path(local_dir, fds_ds$full[i]))
          }), fds_ds$obj)
        } else {
          nlapply(forms, \\(form) {
            fds_dsx <- fds_ds %>% filter(obj == !!form)
            if(nrow(fds_dsx) == 0) stop(paste(form, 'not a REDCap form'))
            read_fxn <- ifelse(fds_dsx$type == 'rds', read_rds, qs_read1)
            read_fxn(file.path(local_dir, fds_dsx$full))
          })
        }
        tm2 = Sys.time()
        message(paste0('Load time: ', format(round(tm2 - tm1, 3), nsmall = 3)))
        return(outfds_list)
        "
        params <- paste0(fds_ds$obj, "=F", collapse=", ")
        fxn_str <- glue("function(..., **params**){**fxn_block**}", 
                        .open="**", .close="**")
        eval(parse(text=fxn_str))
        
      }
      out_list[[fds_list]] <- rds_fdsl_fxngen()
    }
  }
  
  
  out_list
}

## search functions to find specific text in repo files

rfl_list <- nlapply(list.files(recursive = T, full.names = T, pattern=".R$"), function(x) readLines(x, warn = F))

search_files <- function(str, clines=5){
  rfl_lines <- lapply(rfl_list, function(x) {
    lines <- which(grepl(str, x, ignore.case = T))
    if(length(lines) == 0) return(as.character())
    lapply(lines, function(i) x[max(1, i-clines):min(length(x), i+clines)])
  })
  rfl_lines[unlist(lapply(rfl_lines, function(x) length(x) > 0))]
}

`%!in%` <- Negate(`%in%`)

# REDCap can code NAs as "" so this can be useful instead of is.na()

na_or_blank <- function(x) {
  if("Date" %in% class(x)) return(is.na(x))
  is.na(x) | x == ""
}

# automatically apply afmt to a variable by it's name

fize <- function(ds, afmts, fcols){
  if(missing(fcols)) fcols = names(ds)
  ds %>%
    mutate(across(all_of(fcols), \(x) {
      if(cur_column() %in% names(afmts)) {
        afmts[[cur_column()]](x)
      } else {x}
    }))
}



# bucketing to 90 day window (RECOVER specific)

cut_to_fum <- function(x, fct = F){
  dys_div90 = 3 * floor(x / 90)
  dys_mod90 = x %% 90
  fu_month_n <- ifelse(dys_mod90 <= 45, dys_div90, dys_div90 + 3)
  if(!fct) return(fu_month_n)
  lims <- c(0, max(fu_month_n, na.rm=T))
  levs_all <- seq(lims[1], lims[2], 3)
  
  factor(fu_month_n, levs_all)
}

# n (%) printout

npct_fxn <- function(num, denom, na_rep="NA", null_rep="NA", incl_denom=F){
  n_num <- as.numeric(num)
  n_denom <- as.numeric(denom)
  
  case_when(
    n_denom == 0 ~ as.character(null_rep),
    is.na(denom) | is.na(num) ~ as.character(na_rep),
    rep(incl_denom, length(num)) ~ sprintf("%g/%g (%1.0f%%)", n_num, n_denom, 100*n_num/n_denom),
    T ~ sprintf("%g (%1.0f%%)", n_num, 100*n_num/n_denom)
  )
}

as_pct <- function(numb, denom = NA, sprintf_txt = "%.3f") {
  case_when(
    is.na(denom) ~ sprintf(sprintf_txt, numb),
    denom == 0     ~ "",
    T              ~ sprintf(sprintf_txt, numb / denom)
  )
}

# REDCap general data dictionary searcher

dd_find <- function(x, ds_dd_p = ds_dd) {
  ds_dd_p %>% 
    filter(grepl(x, field_name, ignore.case = T) | 
             grepl(x, form_name, ignore.case = T) | 
             grepl(x, field_label, ignore.case = T))
}

# gives exact calendar periods, e.g. as_period("2024-1-1", "2024-2-1", "months") vs as.numeric(as.Date("2024-2-1") - as.Date("2024-1-1"))/30.4 or as_period("2024-1-1", "2025-1-1", "years") vs as.numeric(as.Date("2025-1-1") - as.Date("2024-1-1"))/365.25

as_period <- function(dt1, dt2, unit="years"){
  as.numeric(as.period(interval(as.Date(dt1), as.Date(dt2))), unit=unit)
}

# parallelized, helpful in purrr e.g. core_ex %>%  mutate(max = pmap(across(-id), psum))

psum <- function (...) {
  apply(do.call("cbind", list(...)), 1, function(x) sum(x, na.rm=T))
}

sum_na <- function (x) {
  if (all(is.na(x))) return(NA)
  sum(x, na.rm = T)
}

min_na = function(x, na_vr=NA) {
  if(all(is.na(x))) return(na_vr)
  min(x, na.rm=T)
}

# used to generate mutually exclusive groups from a multiselect. xxxnotxxx used to be filtered out

tab_factor <- function (x, chr, na_cond) {
  x_na <- ifelse(na_cond, NA, as.numeric(x))
  factor(x_na, c(0, 1), c("XXXNOTXXX", chr))
}

# read in data from httr api call without applying types automatically

content_chr <- function(x)  content(x, col_types = cols(.default = "c"))

# reduce levels in a factor to only those that exist in data

lev_reducer <- function(x) factor(x, levels(x)[levels(x) %in% x])

# set rt to latest dated directory
get_fl_fxn <- function(x, type_str, fxn=ymd_hm){
  dt_pt <- suppressWarnings(fxn(str_extract(x, glue("(?<={type_str}).*(?=\\..+$)"))))
  x[!is.na(dt_pt) & dt_pt == max(dt_pt, na.rm = T)]
}

# used for going through list of csv files and loading the one with the most recent date
get_folder_fxn <- function(rt, type_str, fxn=ymd_hm, read=F){
  all_files <- list.files(rt)[grepl(type_str, list.files(rt))]
  loc_out <- get_fl_fxn(all_files, type_str, fxn)
  if(!read) {
    return(loc_out)
  } else {
    read_csv(file.path(rt, loc_out))
  }
}

# get_folder_fxn("/opt/app/home/shared/DM_src/event_mapping/", type_str = "peds_eventmap_id_", fxn = ymd, read=T)

# functions to get data dictionary into a more useful form

# Input:
#   - cur_vrb_col: the column from ds_fdata_char associated with the current vrb
#   - vrb_name: the name of the current variable at hand 
# Output:
#   - the modified (or not) column for this vrb
conv_prop_type <- function(cur_vrb_col, vrb_name, dd=ds_dd, verbose = F) {
  # grab all the info from dd from this vrb_name
  vrb_info <- dd %>% filter(field_name == vrb_name)
  attr(cur_vrb_col, "label") <- vrb_info$field_label
  
  as_logical_numeric <- function(x){
    if(length(setdiff(na.omit(x), c("FALSE", "TRUE"))) == 0){
      case_when(
        x %in% "FALSE" ~ 0,
        x %in% "TRUE" ~ 1,
        T ~ as.numeric(NA)
      )
    } else {
      as.numeric(x)
    }
  }
  
  # the variable is in fdata, but not in the data dictionary
  if (dim(vrb_info)[1] == 0) {
    if(grepl("___\\d+$", vrb_name)) {
      return(as_logical_numeric(cur_vrb_col))
    } else if(grepl("___[a-z]+$", vrb_name, ignore.case = T) & 
              length(setdiff(na.omit(cur_vrb_col), c("FALSE", "TRUE"))) == 0) {
      return(as_logical_numeric(cur_vrb_col))
    }
    return(cur_vrb_col)
  } else if (vrb_info$field_type %in% "radio") {
    # the variable is a non-Odorant, radio button 
    if (verbose) print(vrb_name)
    lablevs <- str_split(str_split(vrb_info$select_choices_or_calculations, "\\|")[[1]], ", ")
    levs <- as.numeric(unlist(lapply(lablevs, "[[", 1)))
    if(any(is.na(levs))){
      return(cur_vrb_col)
    } else {
      return(as.numeric(cur_vrb_col))
    }
  } else if ("calc" %in% vrb_info$field_type | 
             vrb_info$text_validation_type_or_show_slider_number %in% c("number", "integer")) {
    # calculated, numeric variable (including multiselect, checkbox variables)
    if (verbose) print(paste(vrb_name, "->", "numeric"))
    # tmp <- tryCatas_logical_numericch(as_logical_numeric(cur_vrb_col), warning=function(w) browser())
    return(as_logical_numeric(cur_vrb_col))
  } else if ("date_mdy" %in% vrb_info$text_validation_type_or_show_slider_number) {
    # date-oriented variable
    if (verbose) print(paste(vrb_name, "->", "date"))
    return(coalesce(ymd(cur_vrb_col, quiet=T), mdy(cur_vrb_col, quiet=T)))
  } else if ("dropdown" %in% vrb_info$field_type) {
    # if the 'dropdown' type variable has numeric values, convert (otherwise leave as be)
    if (any(!(cur_vrb_col %in% as.numeric(cur_vrb_col)))) {
      return(cur_vrb_col)
    } else {
      return(as.numeric(cur_vrb_col))
    }
  } else return(cur_vrb_col)
}


# REDCap equivalent function for getting the difference between two date-times
datediff <- function(dt1, dt2, units = "d", ...){
  # Search for true false in ... to decide whether or not to abs
  dt_end = suppressWarnings(coalesce(ymd(dt1), mdy(dt1)))
  dt_start = suppressWarnings(coalesce(ymd(dt2), mdy(dt2)))
  as.numeric(difftime(dt_start, dt_end, units = units))
}

#' convert the REDCap branching_logic into an R-friendly string
#' String searches generated as new scenarios rose up. Will need more steps with expanded use
convert_branching_logic <- function(br) {
  baseline_biosex_vrs <- c("[baseline_arm_.][biosex]",
                           "[first-event-name][biosex]", 
                           "[first-event-name][demo_cgbiosex]")
  baseline_biosex_txt <- paste(gsub("\\]", "\\\\]", gsub("\\[", "\\\\[", baseline_biosex_vrs)), collapse = "|")
  
  br %>% 
    str_replace_all(baseline_biosex_txt, "[base_biosex]") %>% 
    str_replace_all("\\((?=\\w)([^\\)]+)\\)\\]", "___\\1]") %>% 
    str_replace_all("\\[previous-event-name\\]\\[visit_bonusoccured\\]", "[prev_visit_bonusoccured]") %>%  # handle the visit_bonusoccured variable logic %>% 
    str_replace_all("\\[previous-event-name\\]\\[visit_dt\\]", "[prev_visit_dt]") %>%  
    str_replace_all("\\[enrollment_arm_1\\]\\[([^\\]]+)\\]\\[first-instance\\]", "[enr_vis_fi_\\1]") %>%  
    str_replace_all("\\[enrollment_arm_1\\]\\[([^\\]]+)\\]\\[last-instance\\]", "[enr_vis_li_\\1]") %>%  
    str_replace_all("\\[enrollment_arm_1\\]\\[([^\\]]+)\\]", "[enr_vis_\\1]") %>%
    str_replace_all("\\[arm-number\\]", "[arm_number]") %>%
    str_replace_all("(?<=[^-])event-name", "redcap_event_name") %>%
    str_replace_all(regex("(?<=\\s)and(?=\\s)", ignore_case = T), "&") %>%
    str_replace_all(regex("(?<=\\s)or(?=\\s)", ignore_case = T), "|") %>%
    str_replace_all("(?<=[^!<>=])\\s?=\\s?(?=[^=])\"\"", " %in% c(\"\", NA)") %>% 
    str_replace_all("(?<=[^!<>=])\\s?=\\s?(?=[^=])", " %in% ") %>%
    str_replace_all("\\s?\"\"\\s?|\\s?''\\s?", " c(\"\", NA) ") %>%  
    str_replace_all("\\s?<>\\s?", " %!in% ") %>%
    str_replace_all("if\\(", "ifelse\\(") %>%
    str_replace_all("current-instance", "redcap_repeat_instance") %>%
    str_replace_all("<-", "< -") %>%
    str_remove_all("[\\[\\]]")
  
}


# create form level branching logic
add_form_branching <- function(dd, in_em, form) {
  
  # add instrument event mapping 
  events <- in_em$unique_event_name[in_em$form == form]
  
  if(length(events) == 0) {
    form_branching = "T"
  } else{
    form_branching <- if("vis_r" %in% names(in_em)) {
      in_em$vis_r[in_em$form %in% form]
    } else {
      paste0("redcap_event_name %in% c('", paste(events, collapse = "', '") ,"')")
    }
  }
  
  # survey queue (so far just pregnancy and covid treatment)
  if (str_detect(form, "pregnancy")){
    sq <- glue("base_biosex %in% '1'")
    
    if (form == "pregnancy_followup"){
      sq <- glue("{sq} & visit_preg_now_copy %in% '1'")
    }
    form_branching <- glue("({form_branching}) & ({sq})")
    
  } else if (form == "covid_treatment"){
    sq <- "(redcap_event_name  %in%  'baseline_arm_1' & cat %in% c(1, 2)) | (str_detect(redcap_event_name, 'followup_') & newinf_yn %in% 1)"
    form_branching <- glue("({form_branching}) & ({sq})")
  }
  
  
  # tier 2/3 tests: use test_*_elig variable
  tests_vrs <- dd %>% 
    filter(str_detect(branching_logic, "_elig") & str_detect(field_name, "_yn")) %>%
    mutate(elig_vr = str_extract(branching_logic, "(?<=\\[)\\w+(?=\\])")) %>%
    select(form_name, elig_vr)
  
  if (form %in% tests_vrs$form_name){
    test_elig <- glue("{tests_vrs$elig_vr[tests_vrs$form_name %in% form]} %in% 1")
    
    form_branching <- glue("({form_branching}) & ({test_elig})")
  }
  
  return(form_branching)
  
}


#' returns a dataset with 1 columns: form name, and logic that determines if the entire form is shown or not
#' can additional integrate survey queue logic (already converted to R)
#' sq_ds <- get_folder_fxn(file.path(dm_loc, "survey_queues"), "pid_36_", fxn=ymd, read=T)
#' sq_ds_r <- sq_ds %>% 
#' mutate(across(condition_logic, \(x) gsub("\\(\\[visit_missed\\]=\"\" or \\[visit_missed\\]=\"0\"\\)", "(T)", x))) %>% 
#' get_rlogic()

get_form_level_branching <- function(dd, in_em, sq_r=data.frame()) {
  
  forms_all <- unique(dd$form_name)
  
  # tier 2/3 tests: use test_*_elig variable
  tests_vrs <- dd %>% 
    filter(str_detect(branching_logic, "_elig$") & str_detect(field_name, "_yn")) %>%
    mutate(elig_vr = str_extract(branching_logic, "(?<=\\[)\\w+(?=\\])")) %>%
    select(form_name, elig_vr)
  
  ds_out_list <- lapply(forms_all, function(form) {
    events <- in_em$unique_event_name[in_em$form %in% form]
    if(length(events) == 0) {
      form_branching = "T"
    } else{
      form_branching <- if("vis_r" %in% names(in_em)) {
        paste0("(", paste(in_em$vis_r[in_em$form %in% form], collapse=" | "), ")")
      } else {
        paste0("redcap_event_name %in% c('", paste(events, collapse = "', '") ,"')")
      }
    }
    # survey queue (so far just pregnancy and covid treatment)
    if (str_detect(form, "pregnancy")){
      sq <- glue("base_biosex %in% '1'")
      
      if (form %in% "pregnancy_followup"){
        sq <- glue("{sq} & visit_preg_now_copy %in% '1'")
      }
      form_branching <- glue("({form_branching}) & ({sq})")
      
    } else if (form %in% "covid_treatment"){
      sq <- "(redcap_event_name %in% 'baseline_arm_1' & cat %in% c(1, 2)) | (str_detect(redcap_event_name, 'followup_') & newinf_yn %in% 1)"
      form_branching <- glue("({form_branching}) & ({sq})")
    } else if (form %in% tests_vrs$form_name){
      test_elig <- glue("{tests_vrs$elig_vr[tests_vrs$form_name %in% form]} %in% 1")
      
      form_branching <- glue("({form_branching}) & ({test_elig})")
    }
    
    if(nrow(sq_r) > 0){
      if(form %in% sq_r$form_name){
        form_branching <- glue("({form_branching}) & ({sq_r$clogic_r[sq_r$form_name %in% form]})")
      }
    }
    
    data.frame(form_name = form, 
               show_form_if = form_branching)
  })
  
  bind_rows(ds_out_list)
}


# returns 2 data frames in a list
# 1. data frame with 1 row per checkbox variable, a column with a list of all the checkbox options, and a column with logic to check if any options were checked
# 2. data frame with data dictionary variable name and all the corresponding variable column names in the REDCap data
get_checkbox_options <- function(dd) {
  # if check all that apply, create individual variable names to match ds_fdata columns
  
  checkbox_options_long <- dd %>%
    filter(field_type == "checkbox") %>%
    mutate(full_list = str_split(select_choices_or_calculations, "\\s?\\|\\s?")) %>% 
    unnest(full_list) %>% 
    mutate(lvl = gsub("-", "_", gsub(",.+", "", full_list)),
           lbl = gsub("^[^,]+, ", "", full_list),
           field_name.ind = paste(field_name, lvl, sep="___"))
  
  checkbox_options_any <- checkbox_options_long %>% 
    group_by(field_name) %>% 
    summarise(any_checked = glue("({paste(glue(\"({field_name}___{lvl} %in% 1)\"), collapse=' | ')})"))
  
  return(list(checkbox.any.checked = checkbox_options_any,
              checkbox.ind.vars = checkbox_options_long %>% 
                select(field_name, field_name.ind,
                       ms_lvl=lvl, ms_lbl=lbl)))
}

repeat_instr_fxn <- function(dsf){
  dsf %>% 
    select(redcap_repeat_instrument) %>%
    distinct() %>% 
    filter(!na_or_blank(redcap_repeat_instrument)) %>%
    mutate(repeat_instr = glue("redcap_repeat_instrument %in% '{redcap_repeat_instrument}'")) 
}

# dd = adult_env_list$ds_dd(); fdata_nm= "ds_fdata"; cohort_nm = "Adult"; in_em = adult_env_list$ds_eventmap_comp(); sq_r=adult_env_list$sq_ds_r(); 
ddprep_fxn <- function(dd, repeat_instr, cohort_nm, in_em = ds_em, sq_r=data.frame()) {
  
  # get logic for showing/hiding entire forms
  form_branching <- get_form_level_branching(dd, in_em, sq_r) %>%
    left_join(repeat_instr, by = c("form_name" = "redcap_repeat_instrument")) %>%
    mutate(repeat_instr = ifelse(is.na(repeat_instr),
                                 "na_or_blank(redcap_repeat_instrument)",
                                 repeat_instr)) 
  
  ### pick up embedded vars from field_label and their branching logic
  embedded_vars <- dd %>% filter(field_type == "descriptive" & str_detect(field_label, "\\{")  & !is.na(branching_logic))
  
  if (nrow(embedded_vars) > 0) {
    ### extract each embedded vars
    embedded_vars_all <- str_extract_all(embedded_vars$field_label, "\\{(\\w+)")
    
    emb <- lapply(1:length(embedded_vars_all), function(i){
      if(length(embedded_vars_all[[i]]) == 0) return(NULL)
      embedded_vars[rep(i, length(embedded_vars_all[[i]])), ] %>% 
        select(branching_logic_from_ET = branching_logic) %>% 
        mutate(field_name = gsub("^\\{", "", embedded_vars_all[[i]]))
    }) %>% 
      bind_rows()
    ### mapping with their branching logic from descriptive type
  } else {
    emb <- data.frame(field_name = as.character(), 
                      branching_logic_from_ET = as.character())
    
  }
  
  ### compare with ds_dd$branching_logic
  ## dd[ dd$field_name %in% emb$field_name, c('field_name','branching_logic')] %>% View()
  
  ### join and mutate
  
  # add R-readable branching logic to DD and include embedded table work
  dd_branch <- left_join(dd, emb, by = "field_name") %>% 
    mutate(branching_logic = case_when(
      is.na(branching_logic)          ~ branching_logic_from_ET,
      !is.na(branching_logic_from_ET) ~ sprintf("(%s) AND (%s)", branching_logic, branching_logic_from_ET),
      T                               ~ branching_logic)) %>% 
    mutate(branching_txt = case_when(
      field_type != "descriptive"  ~ convert_branching_logic(branching_logic) 
    )) %>%
    left_join(form_branching, by = "form_name") %>%
    mutate(branching_with_form = ifelse(!is.na(branching_logic),
                                        glue("({show_form_if}) & ({repeat_instr}) & ({branching_txt})"),
                                        glue("({show_form_if}) & ({repeat_instr})")) %>% 
             str_replace_all("cc_fversion *< *3", "(cc_fversion<3 | is.na(cc_fversion))"))
  
  
  
  checkbox.ds <- get_checkbox_options(dd)
  
  # expand dd to have all individual checkbox variables (save to rt directory)
  dd_big <- dd_branch %>%
    full_join(checkbox.ds$checkbox.ind.vars, 
              by = "field_name",
              multiple="all") %>%
    mutate(field_name_src = field_name, 
           field_name = ifelse(!is.na(field_name.ind), field_name.ind, field_name)) %>%
    select(-field_name.ind) 
  
  # exclude descriptive fields and hidden fields before running autodd_queries.R (no need to check values/missingness)
  dd_val <- dd_branch %>%
    filter(field_type != "descriptive") %>%
    filter(!(grepl("@HIDDEN", field_annotation) & !grepl("@HIDDEN-SURVEY", field_annotation)) | 
             grepl("_calc$", field_name) | grepl("_musicul$", field_name))  %>%
    left_join(checkbox.ds$checkbox.any.checked, by = "field_name")
  
  # fversion to form name mapping (may need to check if form was ever saved)
  form_fversions <- dd %>%
    filter(str_detect(field_name, "_fversion$")) %>%
    select(field_name, form_name)
  
  list(dd_big = dd_big, dd_val = dd_val, form_fversions = form_fversions)
}

clogic_conv <- function(x){
  x %>% 
    str_replace_all("\\[visit_form_complete\\]=\"+\"", "T") %>% 
    str_replace_all("\\[next-event-name\\]\\[visit_dt\\]=\"\"", "T") %>% 
    str_replace_all("\\[\\d+_month_arm_\\d+\\]\\[visit_dt\\]=\"\"", "T") %>% 
    str_replace_all("\\[visit_form_complete\\]=\"2\"", "T") %>% 
    str_replace_all("NULL", "T")
}


get_sqrlogic <- function(ds){
  
  ds %>%
    mutate(clogic = clogic_conv(condition_logic),
           vistype = ifelse(grepl("baseline", event_name, ignore.case = T), "B", "F")) %>%
    summarise(all_vis = ifelse(any(event_name %in% c('', NA)), "T", paste0("(redcap_event_name %in% c(", paste(paste0("'", unique(event_name), "'"), collapse=", "), "))")),
              .by=c(form_name, clogic, FD)) %>% 
    mutate(clogic_r = glue("(({convert_branching_logic(clogic)}) & {all_vis})")) %>% 
    summarise(clogic_r = paste0(clogic_r, collapse=" | "),
              .by=c(form_name, FD)) %>% 
    summarise(clogic_r = paste0(clogic_r, collapse=" | "),
              .by=form_name)
  
} 



form_read_fxn <- function(str, flnms, rt_str, ds_dd_coh) {
  
  fl_loc <- get_fl_fxn(flnms, paste0("_DATA_", str, "_"))
  
  if(grepl("qs2$", fl_loc)){
    ds_out <- qs_read(file.path(rt_str, fl_loc))
  } else {
    ds_out <- read.csv(file.path(rt_str, fl_loc), colClasses="character")
  }
  
  
  
  all_numeric_type_vrbs <- ds_dd_coh %>%
    filter(text_validation_type_or_show_slider_number %in% c("number", "integer")) %>%
    pull(field_name)
  
  if (length(intersect(colnames(ds_out), all_numeric_type_vrbs)) == 0) { 
    ds_num_chr_issues <- data.frame() # if there are no variables in this current form's ds are numeric 
  } else {
    ds_num_chr_issues <- ds_out %>% 
      select(any_of(c("record_id", "redcap_event_name", "redcap_repeat_instance", "redcap_repeat_instrument", all_numeric_type_vrbs))) %>% 
      pivot_longer(cols = c(-record_id, -redcap_event_name, -redcap_repeat_instrument, -redcap_repeat_instance), 
                   names_to = "variable", 
                   values_to = "value") %>% 
      mutate(value_conv_num = as.numeric(value)) %>%
      filter((!na_or_blank(value)) & (is.na(value_conv_num)))
  }
  
  ds_out_pc <- ds_out %>% 
    mutate(across(everything(), \(x) conv_prop_type(x, cur_column(), dd=ds_dd_coh))) %>% 
    select(-any_of("redcap_survey_identifier"))
  
  list(nc_issues = ds_num_chr_issues, ds = ds_out_pc %>% mutate(form = str))
}

# Read in arguments from shell script
getArgs <- function(verbose = FALSE, defaults = NULL) {
  
  myargs <- gsub("^--", "", commandArgs(TRUE))
  setopts <- !grepl("=", myargs)
  if(any(setopts))
    myargs[setopts] <- paste(myargs[setopts],"=notset", sep = "")
  myargs.list <- strsplit(myargs, "=")
  myargs <- lapply(myargs.list, \(x){
    if(length(x) == 1) return(NA)
    x[[2]]
  })
  names(myargs) <- lapply(myargs.list, "[[", 1)
  
  ## logicals
  if(any(setopts))
    myargs[setopts] <- TRUE
  
  ## defaults
  if(!is.null(defaults)) {
    defs.needed <- setdiff(names(defaults), names(myargs))
    if(length(defs.needed)) {
      myargs[ defs.needed ] <- defaults[ defs.needed ]
    }
  }
  
  ## verbage
  if(verbose) {
    cat("read", length(myargs), "named args:\n")
    print(myargs)
  }
  myargs
}


get_fmt <- function(chs){
  lapply(strsplit(chs, "\\|"), function(x) {
    pts <- strsplit(x, ",")
    
    levs <- tryCatch(as.numeric(unlist(lapply(pts, "[[", 1))),
                     warning=function(w) gsub("^ | $", "", unlist(lapply(pts, "[[", 1))))
    
    labels <- gsub("^ | $", "", unlist(lapply(pts, function(x) paste(x[-1], collapse=","))))
    ds_out <- data.frame(levs = levs, labels=labels)
    if(class(levs) == "numeric") { 
      glue("function(x) factor(x, levels= c({paste(levs, collapse=', ')}), labels=c({paste0(\"'\", gsub(\"'\", \"\", labels), \"'\", collapse=', ')}))")
    } else {
      glue("function(x) factor(x, levels= c({paste0(\"'\", levs, \"'\", collapse=', ')}), labels=c({paste0(\"'\", gsub(\"'\", \"\", labels), \"'\", collapse=', ')}))")
    }
  })
}

# generates afmts for each project
fmt_gen_fxn <- function(ds){
  
  fmts <- ds %>% 
    filter(field_type %in% c("checkbox", "radio", "dropdown")) %>% 
    mutate(fmt_txt = get_fmt(select_choices_or_calculations))
  
  nilapply(tolower(fmts$field_name), function(i){
    fxn_desc <- fmts[i, "fmt_txt"][[1]]
    
    res_levs <- str_match(fxn_desc[[1]], "levels=\\s*(.*?)\\s*, labels=")
    levs <- eval(parse(text = res_levs[,2]))
    
    res_labs <- paste0(str_match(fxn_desc[[1]], "labels=\\s*(.*?)\\s*\\'\\)\\)"), "')")
    labs <- eval(parse(text = res_labs[2]))
    fp_trib <- data.frame(levs = paste0(levs, ","), labs = paste0("'", labs, "'"))
    fp_df <- tibble(levs = levs, labs = labs)
    fxn_out <- eval(parse(text = fxn_desc))
    attr(fxn_out, "tribble") <- fp_trib
    attr(fxn_out, "tibble") <- fp_df
    attr(fxn_out, "vctr") <- setNames(labs, levs)
    
    fxn_out
  })
}

# create query categories

dq_summ <- function(ds, internal_users = c()){
  ds %>% 
    filter(!is.na(res_id),
           query_status %in% c("CLOSED", "OPEN")) %>%
    mutate(drc_involved = username %in% internal_users) %>% 
    mutate(any_drc_involved = any(drc_involved),
           .by=status_id) %>% 
    filter(any_drc_involved) %>% 
    summarise(date_init = as.Date(ts[1]),
              date_closed = if_else(!any(current_query_status == "CLOSED"), NA, as.Date(ts[current_query_status == "CLOSED"][1])),
              step_site = ifelse(n() == 1 & !any(query_status == "CLOSED"), "2. Awaiting site", NA),
              step_closed1 = ifelse(any(query_status == "CLOSED") & !drc_involved[n()], "3a. Closed by site", NA),
              step_closed2 = ifelse(any(query_status == "CLOSED"), "3b. Closed by DRC", NA),
              step_disc = ifelse(n() > 1 & is.na(response[n()]), "4. Discussion Ongoing", NA),
              step_drc = ifelse(n() > 1, "5. Awaiting DRC", NA),
              ex_handling = any(grepl("exception handling", comment)),
              level_form = ifelse(any(grepl("Form expected", comment)), "Full form query", NA),
              level_visit = ifelse(any(grepl("Visit Incomplete", comment)), "Full visit query", NA),
              type_missing = ifelse(any(grepl("Data expected|Visit Incomplete|Form expected|DRCX2", comment)) |
                                      any(grepl("4 or more weeks past enrollment", comment) & field_name == "visit_dt"), "Missingness query", NA),
              .by=c(record, field_name, status_id)) %>% 
    mutate(drc_status = coalesce(step_site, step_closed1, step_closed2, step_disc, step_drc),
           level_desc = coalesce(level_visit, level_form, "Variable query"),
           type_desc = coalesce(type_missing, "Validation query"),
           time_to_close = round(date_closed - date_init))
}



add_daily_log <- function(task, flnm, loc = output_loc, note=""){
  
  rc_loc <- gsub("reports/.+", glue("reports/run_checks/{flnm}.csv"), loc)
  write.csv(data.frame(task=task, 
                       success=T, 
                       dttm= format(Sys.time(),"%Y%m%d_%H%M"), 
                       note=note),
            rc_loc,
            row.names=F)
  print(glue("{task} written out to {rc_loc}"))
}


docx_head_fxn <- function(off_obj, txt, font.size, color_hex){
  cc_heading <- fp_text(color = color_hex,
                        bold=T, 
                        # font.family = "MV Boli",
                        font.size = font.size)
  
  head1_obj <- ftext(txt, cc_heading) %>% fpar()
  
  body_add(off_obj, head1_obj, style = "heading 1")
}

docx_head1 <- function(off_obj, txt) docx_head_fxn(off_obj, txt, font.size = 18, color_hex = "#4472c0")
docx_head2 <- function(off_obj, txt) docx_head_fxn(off_obj, txt, font.size = 16, color_hex = "#4472c0")


body_add_flextable_font <- function(doc_obj, flt) {
  if("data.frame" %in% class(flt)) {
    flt_obj = printtab(flt, footnote=F)
  } else {
    flt_obj <- flt
  }
  flt_fmtd <- flt_obj %>% 
    hline(i=1, border = fp_border(color = "black"), part="header") %>% 
    vline(j=1, border = fp_border(color = "black"), part="header") %>% 
    vline(j=1, border = fp_border(color = "black"), part="body") %>% 
    fontsize(size = 9, part = "header") %>% 
    fontsize(size = 9, part = "body") %>% 
    fontsize(size = 8, part = "footer") %>% 
    align(align = c("left"), part = c("body")) %>% 
    align(align = c("left"), part = c("header"))
  
  body_add_flextable(doc_obj, flt_fmtd, align="left")
}

# These would only work if Ricky continued to use RECOVER style lab forms in future projects. If that is the case then these should be kept and modified. otherwise these can be deleted

lab_info_fxn <- function(ds, ds_extra=data.frame(field_name=as.character(), cf=as.character())){
  ds %>% 
    filter(grepl("labinfo", field_annotation)) %>% 
    mutate(lab_info_str = gsub(".+labinfo=(.+)\\s*", "\\1", field_annotation),
           lab_info_sep = str_split(lab_info_str, ";")) %>% 
    unnest(lab_info_sep) %>% 
    separate_wider_delim(lab_info_sep, delim=":", names=c("vr", "val")) %>% 
    select(field_name, vr, val) %>% 
    distinct() %>% 
    pivot_wider(names_from = vr, values_from = val) %>% 
    mutate(lab_nm = gsub("^r|^c|^t3", "", field_name)) %>% 
    left_join(ds_extra,
              by = join_by(field_name)) %>% 
    mutate(across(conversionfactor, \(x) coalesce(x, as.character(cf)))) %>% 
    left_join(ds %>% 
                filter(form_name %in% c("research_labs", "clinical_labs", 'tier_3_labs')) %>% 
                select(field_name, unit_note = field.note),
              by = join_by(field_name)) %>% 
    mutate(across(lab, \(x) ifelse(grepl("trop.+ul", field_name), paste0(x, "ul"),x)))
}

mk_labs_long <- function(ds){
  ds %>% 
    select(-any_of(c('redcap_repeat_instrument', 'redcap_repeat_instance', 'form')),
           -matches('biosex'), 
           -matches('fversion'),
           -matches('fqueries'),
           -matches("_complete")) %>% 
    mutate(across(everything(), as.character)) %>% 
    pivot_longer(cols=-c(record_id, redcap_event_name),
                 names_to = 'field_name', 
                 values_to = 'lab_val') %>% 
    filter(!is.na(lab_val) & !(grepl("nn___1$", field_name) & lab_val == 0))
}

labs_long_to_wide <- function(ds, lab_vr){
  ds %>% 
    filter(!is.na(!!sym(lab_vr))) %>% 
    select(record_id, redcap_event_name, panel, lab, val=!!sym(lab_vr), src, dt=date_val) %>% 
    pivot_wider(names_from=c(panel, lab), values_from=c(val, src, dt),
                names_glue = "{panel}_{lab}_{.value}",
                names_vary = "slowest") %>% 
    arrange(record_id, redcap_event_name)
}

labs_list_ds <- function(ll, info_ds){
  lab_info_unique_check <- info_ds %>% 
    select(lab_nm, conversionfactor, unit_note, panel, lab) %>% 
    distinct() %>% 
    filter(n() > 1, .by=lab_nm) %>% 
    arrange(lab_nm)
  
  lab_info_unique <- info_ds %>% 
    mutate(across(conversionfactor, \(x) ifelse(lab_nm %in% c("", "lab_glucnnv", "lab_glucnn___1", "lab_glucab", "lab_altnnv", "lab_altnn___1", "lab_altab"), NA, x))) %>% 
    select(lab_nm, conversionfactor, unit_note, panel, lab) %>% 
    filter(row_number() == 1, .by=lab_nm)
  
  labs_long0 <- bind_rows(ll) %>% 
    mutate(lab_nm = gsub("^r|^c|^t3", "", field_name))
  
  labs_long <- labs_long0 %>% 
    left_join(info_ds %>% 
                select(field_name, date_vr=date) %>% 
                filter(!is.na(date_vr)),
              by=join_by(field_name == field_name)) %>%
    left_join(labs_long0 %>% 
                select(record_id, redcap_event_name, date_vr= field_name, date_val = lab_val) %>% 
                distinct(),
              by = join_by(record_id, redcap_event_name, date_vr)) %>% 
    mutate(src= gsub("lab_.+", "", field_name)) %>% 
    select(-field_name) %>% 
    filter(!is.na(lab_val)) %>% 
    filter(!(n() > 1 & src == "c"),
           .by=c(record_id, redcap_event_name, lab_nm)) %>% 
    left_join(lab_info_unique,
              by = join_by(lab_nm)) %>% 
    filter(!is.na(lab_val)) %>% 
    mutate(lab_cval = as.numeric(lab_val) * as.numeric(conversionfactor),
           lab_cval_chr = ifelse(grepl("nnv$", lab_nm), lab_val, as.character(lab_cval)))
  
  
  labs_wide <- labs_long_to_wide(labs_long, "lab_cval")
  labs_wide_chr <- labs_long_to_wide(labs_long, "lab_cval_chr")
  
  list(long=labs_long, wide=labs_wide, wide_chr=labs_wide_chr)
}

piv_lab_form <- function(ds, prefix, comp_vr, prefix_gen= "lab", extra_piv_vrs=NULL){
  ds %>% 
    select(-any_of(c('redcap_repeat_instrument', 'redcap_repeat_instance', 'form', paste0(prefix, c('_biosex', '_fversion', '_fqueries')), comp_vr))) %>% 
    mutate(across(everything(), as.character)) %>% 
    pivot_longer(cols=-all_of(c('record_id', 'redcap_event_name', extra_piv_vrs)),
                 names_to = paste0(prefix, "_nm"), 
                 values_to = paste0(prefix, "_val")) %>% 
    filter(!is.na(!!sym(paste0(prefix, "_val")))) %>% 
    mutate(lab_nm = gsub(paste0("^", prefix), prefix_gen, !!sym(paste0(prefix, "_nm")))) %>% 
    filter(!(grepl("nn___1$", lab_nm) & !!sym(paste0(prefix, "_val")) == 0))
}

add_wbc_fxn <- function(ds){
  if(!any(ds$lab_nm %in% "lab_wbc")){
    return(ds %>% mutate(wbc_val = NA))
  } 
  
  ds %>% 
    left_join(ds %>% 
                filter(lab_nm == "lab_wbc") %>% 
                select(record_id, redcap_event_name, wbc_val = lab_val) %>% 
                filter(row_number() == 1, 
                       .by=c(record_id, redcap_event_name)) %>% 
                mutate(across(wbc_val, as.numeric)),
              by=join_by(record_id, redcap_event_name))
}


mk_labs_comb_long <- function() {
  # This is only a function to avoid saving interim datasets. Could also do this with rm statements
  
  clab_long <- piv_lab_form(formds_list$clinical_labs, "clab", "clinical_labs_complete") %>% 
    left_join(lab_unit_info, by=join_by(clab_nm == field_name))
  
  ds_dts <- clab_long %>% 
    right_join(lab_dates,
               by=join_by(clab_nm==date)) %>% 
    select(record_id, redcap_event_name, panel, clab_dt=clab_val)
  
  
  rlab_long <- piv_lab_form(formds_list$research_labs, "rlab", "research_labs_complete", extra_piv_vrs = "rlab_dt") %>% 
    left_join(lab_unit_info, by=join_by(rlab_nm == field_name))
  
  
  clab_long %>%
    select(-unit_note) %>% 
    filter(clab_nm %!in% lab_dates$date) %>%
    full_join(rlab_long,
              by = join_by(record_id, redcap_event_name, lab_nm)) %>%
    left_join(lab_panels,
              by = join_by(lab_nm)) %>%
    left_join(ds_dts,
              by = join_by(record_id, redcap_event_name, panel)) %>%
    left_join(formds_list$visit_form %>%
                select(record_id, redcap_event_name, visit_dt),
              by = join_by(record_id, redcap_event_name)) %>%
    mutate(src = ifelse(any(!is.na(rlab_val[!grepl("___\\d", lab_nm)])), "research", "clinical"),
           .by=c(record_id, redcap_event_name, panel)) %>%
    mutate(lab_val = ifelse(src %in% "research", rlab_val, clab_val),
           lab_dt = ifelse(src %in% 'research', coalesce(rlab_dt, format(visit_dt, "%Y-%m-%d")), clab_dt)) %>%
    select(-c(clab_nm, rlab_nm, visit_dt)) %>%
    filter(!is.na(lab_val)) %>%
    arrange(record_id, redcap_event_name, lab_nm) %>%
    left_join(conv,
              by = join_by(lab_nm)) %>% 
    add_wbc_fxn() %>% 
    mutate(cf_num = case_when(grepl("wbc_val", conversionfactor) ~ 100/wbc_val,
                              .default = as.numeric(conversionfactor)))
  
}

## end labs code ------


# helpful if external groups asks for data dictionary printout

dd_printr <- function(dd){
  if("variable...field.name" %in% names(dd) | 
     "Variable...Field.Name" %in% names(dd)){
    names(dd) <- gsub("\\.+$", "", gsub("\\.+", "\\.", tolower(names(dd))))
    dd$branching_logic = dd$branching_logic.show.field.only.if
    dd$field_name = dd[[1]]
  }
  dd %>% 
    filter(field_type != "descriptive") %>% 
    mutate(field_label.short = gsub("<img style.+/>", "", field_label),
           val_txt = case_when(!is.na(text_validation_type_or_show_slider_number) ~ glue("Validation [special]: {text_validation_type_or_show_slider_number}"),
                               !is.na(text_validation_min) & !is.na(text_validation_max) ~ glue("Validation: min = {text_validation_min}, max = {text_validation_max}"),
                               !is.na(text_validation_min) ~ glue("Validation: min = {text_validation_min}"),
                               !is.na(text_validation_max) ~ glue("Validation: max = {text_validation_max}")),
           across(field_annotation, \(x) gsub("\\|", "|\n", x)),
           attributes = glue("{field_type}{ifelse(required.field %in% 'y', ', Required', '')}\n",
                             "{ifelse(!is.na(val_txt), paste0(val_txt, '\n'), '')}",
                             "{ifelse(!is.na(select_choices_or_calculations) & field_type == 'calc', 'Calculation: ', '')}",
                             "{ifelse(!is.na(select_choices_or_calculations) & field_type == 'calc', select_choices_or_calculations, '')}",
                             "{ifelse(!is.na(select_choices_or_calculations) & field_type == 'calc', '\n', '')}",
                             "{ifelse(!is.na(select_choices_or_calculations) & field_type %in% c('checkbox', 'radio', 'dropdown'), 'Options: ', '')}",
                             "{ifelse(!is.na(select_choices_or_calculations) & field_type %in% c('checkbox', 'radio', 'dropdown'), select_choices_or_calculations, '')}",
                             "{ifelse(!is.na(select_choices_or_calculations) & field_type %in% c('checkbox', 'radio', 'dropdown'), '\n', '')}",
                             "Field Annotation: {replace_na(field_annotation, '')}"),
           across(attributes, \(x) gsub("\\[", " [", x)),
           across(attributes, \(x) gsub("/", " /", x)),
           across(attributes, \(x) gsub(";", "; ", x)),
           vr.text = ifelse(!is.na(branching_logic), glue("{field_name}\n\nShow the field ONLY if:\n{branching_logic}"), field_name)) %>% 
    select(form_name, vr.text, field_label=field_label.short, attributes)
}


seq_cntr <- function(vr_tf){
  if(length(vr_tf) == 1) return(vr_tf %!in% T)
  vr_tf_out <- rep(NA, length(vr_tf))
  vr_tf_out[1] <- as.numeric(vr_tf[1] %!in% T)
  for(i in 2:length(vr_tf)) {
    if(vr_tf[i] %!in% T & vr_tf[i-1] %!in% T) {
      vr_tf_out[i] <- vr_tf_out[i-1] + 1
    } else {
      vr_tf_out[i] <- as.numeric(vr_tf[i] %!in% T)
    }
  }
  vr_tf_out
}


ord_match_fxn <- function(ds, crits, recycle=T, fixed_n = 1, n_matches, ds_match){
  if(missing(ds_match)) {
    ds_match = setNames(data.frame(t(rep(T, length(crits)))), c(crits)) %>% mutate(record_id = "1")
  }
  if(missing(n_matches)) n_matches = nrow(ds)
  if(recycle) {
    iterations <- lapply(1:(2^length(crits)), \(x) abs(rev(as.integer(intToBits(x)[1:length(crits)])) - 1))
    iterations_all = c(list(rep(1, length(crits))), iterations[-1*length(iterations)])
  } else {
    iterations <- rep(1, length(crits))
    iterations_all = lapply(0:(length(crits)), \(x) {
      iterations[0:x] <- 0
      rev(iterations)
    })
  }
  
  iterations_use <- lapply(iterations_all, \(x) {
    x[1:fixed_n] <- 1
    x
  }) %>% 
    unique()
  
  desc_tab <- setNames(bind_rows(lapply(iterations_use, \(x) data.frame(t(x)))), 
                       crits)
  
  match_iter <- ds_match %>% 
    mutate(match_n_rem = !!n_matches)
  
  ds_rem <- ds
  
  all_matches <- ds_match[0, ]
  
  for(j in 1:length(iterations_use)){
    iter_x <- iterations_use[[j]]
    crits_use <- crits[as.logical(iter_x)]
    pos_matches <- lapply(1:nrow(match_iter), \(i){
      crits_flags <- match_iter[i, crits_use]
      if(length(crits_flags) == 0){
        crit_string <- "T"
      } else {
        crit_string <- paste0("(", crits_use, " %in% ", crits_flags, ")", collapse=" & ")
      }
      
      
      ds_out <- ds_rem %>% 
        filter(eval(parse(text= crit_string)))
      if(nrow(ds_out) == 0) {
        return(bind_cols(ds_out, data.frame(match_n=1, match_id = "", match_n_rem=1)[0, ]))
      } else {
        return(ds_out %>% 
                 mutate(match_n = 1:n(), 
                        match_id = match_iter$record_id[i],
                        match_n_rem = match_iter$match_n_rem[i]))
      }
    }) %>% 
      bind_rows() %>% 
      filter(!duplicated(record_id))
    
    if(nrow(pos_matches) == 0) next()
    
    match_out <- pos_matches[0, ]
    n_match_iter = T
    i=1
    while(n_match_iter){
      complete_records = match_out %>% 
        summarise(n = n(),
                  .by=c(match_id, match_n_rem)) %>% 
        filter(n == match_n_rem) %>% 
        pull(match_id)
      
      match_out <- bind_rows(match_out, 
                             pos_matches %>% 
                               filter(match_n == i,
                                      match_id %!in% complete_records) %>% 
                               filter(!duplicated(record_id)))
      i= i+1
      if(length(complete_records) == length(unique(match_out$record_id)) | i > max(pos_matches$match_n)) n_match_iter = F
    }
    
    match_iter <- match_iter %>% 
      left_join(match_out %>% 
                  summarise(n_matchsub = n(),
                            .by=match_id),
                by=join_by(record_id==match_id)) %>% 
      mutate(match_n_rem = match_n_rem - n_matchsub) %>% 
      select(-n_matchsub) %>% 
      filter(match_n_rem > 0)
    
    ds_rem <- ds_rem %>% 
      filter(record_id %!in% match_out$record_id)
    
    all_matches <- bind_rows(all_matches, 
                             match_out %>% 
                               mutate(match_crit = paste(crits_use, collapse=", ")))
    if(nrow(match_iter) == 0) break()
  }
  return(list(ds = all_matches, desc=desc_tab))
}







# Manual: https://github.com/vanderbilt-redcap/data_quality_api
dq_url <- function(urlapi, pg, pid) glue("{urlapi}?prefix=data_quality_api&page={pg}&pid={pid}&type=module&NOAUTH")




push_query <- function(urlapi, token, pid, evnt_nm, evnt_id,
                       instrument, instance, rid, fnm, 
                       res_id, status_id="",
                       comment="", status="OPEN", verbose=F){
  dt_tm <- format(Sys.time(), "%Y-%m-%d %H:%M:01")
  res_id <- ifelse(missing(res_id), "", res_id)
  instance_1 <- ifelse(is.na(instance) | is.null(instance), 1, instance)
  
  if(missing(evnt_id)){
    event_param = "event_name"
    event_name = evnt_nm
  } else {
    event_param = "event_id"
    event_name = evnt_id
  }
  
  query_in <- glue("{
    \"\": {
        \"status_id\": \"{{status_id}}\",
        \"rule_id\": null,
        \"pd_rule_id\": null,
        \"non_rule\": \"1\",
        \"project_id\": \"{{pid}}\",
        \"record\": \"{{rid}}\",
        \"{{event_param}}\": \"{{event_name}}\",
        \"field_name\": \"{{fnm}}\",
        \"instance\": \"{{instance_1}}\", 
        \"status\": null,
        \"exclude\": \"0\",
        \"group_id\": \"null\",
        \"assigned_username\": \"\",
        \"resolutions\": {
            \"\": {
                \"res_id\": \"{{res_id}}\",
                \"status_id\": \"{{status_id}}\",
                \"ts\": \"{{format(Sys.time(), '%Y-%m-%d %H:%M:%S')}}\",
                \"response_requested\": \"1\",
                \"response\": null,
                \"comment\": \"{{comment}}\",
                \"current_query_status\": \"{{status}}\",
                \"upload_doc_id\": null,
                \"field_comment_edited\": \"0\",
                \"username\": \"jy70\"
            }
        }
    }
}", .open = "{{", .close = "}}")
  formData <- list("token"=token,
                   format='json',
                   returnFormat='json',
                   data=query_in
  )
  
  if(verbose) {
    response <- POST(
      url=urlapi,
      body=formData,
      verbose()
    )
  } else {
    response <- POST(
      url=urlapi,
      body=formData
    )
  }
  
  result <- content(response)
  rawToChar(result)
}


pull_queries <- function(urlapi, token, verbose=F, records){
  
  formData <- list("token"=token,
                   format='json',
                   returnFormat='json'
  ) 
  # Form multiple specific IDs add to call
  # "record[0]"="RA11501-00227",
  # "record[1]"="RA12101-00017", ... etc
  # Could limit this call to just those IDs in new queries to avoid long run.
  if(!missing(records)){
    i = 0
    for(record in records){
      rlist <- list(record)
      names(rlist) <- paste0("record[", i, "]")
      formData <- c(formData, rlist)
      i = i + 1
    }
  }
  
  
  response <- if(verbose){
    POST(
      url=urlapi,
      body=formData,
      verbose()
    )
    
  } else {
    POST(
      url=urlapi,
      body=formData
    )
  }
  
  result <- content(response)
  
  jsonlite::prettify(rawToChar(result))
}

push_query_coh <- function(queries_ds_in, tk, pid, rt_url, status= "OPEN", ds_dd, incl_dup = F, use_id = F){
  
  if(any(is.na(queries_ds_in$form)) & missing(ds_dd)) stop("Some queries missing form information. Add ds_dd as parameter to run this function") 
  queries_ds <- queries_ds_in %>% 
    select(-any_of("form")) %>% 
    left_join(ds_dd %>% 
                select(variable = field_name, form=form_name),
              by="variable")
  
  if(pid == 38) queries_ds$redcap_event_name <- "event_1_arm_1"
  if(!incl_dup) queries_ds$status_id <- ""
  q_url <- dq_url(rt_url, "import", pid)
  if("redcap_repeat_instance" %!in% names(queries_ds)) queries_ds$redcap_repeat_instance = 1
  
  unlist(lapply(1:nrow(queries_ds), function(i){
    push_query(q_url, tk, pid, 
               evnt_id = queries_ds$event_id[i], 
               instrument = queries_ds$form[i], 
               instance = queries_ds$redcap_repeat_instance[i], 
               rid = queries_ds$record_id[i], 
               fnm = queries_ds$variable[i], 
               comment = queries_ds$comment[i], 
               status_id = queries_ds$status_id[i], 
               status = status) 
    
  }))
}

dqpush_close <- function(all_dels, tk, ds_dd, ds_qy, evnt, push=T){
  
  if(nrow(all_dels) == 0){
    print("no data in input dataset")
    return(all_dels)
  }
  coh_params <- get_rc_params(tk)
  push_ds <- all_dels %>%
    select(status_id, comment) %>%
    distinct() %>% 
    left_join(ds_qy %>% 
                select(-comment) %>%
                filter(row_number() == n(),
                       .by=status_id)) %>%
    filter(query_status == "OPEN") %>% 
    select(-any_of("redcap_event_name_api")) %>%
    rename(variable = field_name,
           redcap_repeat_instance = instance, 
           record_id = record) %>%
    gen_link_ds(tk, ds_dd, evnt, nm_vr = "variable") 
  
  if(push) push_ds$res_id <- push_query_coh(push_ds, tk, coh_params$pid, coh_params$urlapi, ds_dd=ds_dd, incl_dup = T, use_id = T, status= "CLOSED")
  push_ds
}




upload_data_rc <- function(tk, data_to_upload, ov="normal", 
                           urlapi = "https://redcap.partners.org/redcap/api/", 
                           steps=500, printi=T,
                           fan = "false") {
  
  if("json" %in% class(data_to_upload)) {
    formData <- list(
      "token" = tk,
      content = "record",
      action = "import",
      format = "json",
      type = "flat",
      overwriteBehavior = ov,
      forceAutoNumber = fan,
      data = data_to_upload,
      returnContent = "count",
      returnFormat = "json"
    )
    response <- POST(urlapi, body = formData, encode = "form")
    result <- content(response)
    return(result)
  } else {
    if(nrow(data_to_upload) == 0) {
      print("No data to upload")
      return(0)
    }
    seq_data <- unique(c(seq(0, nrow(data_to_upload), by=steps), nrow(data_to_upload)))
    result_list <- list()
    for(i in 1:(length(seq_data) - 1)){
      if(printi) print(glue("Push {i} of {length(seq_data) - 1}"))
      data_to_upload_json = data_to_upload %>% slice((seq_data[i] + 1):seq_data[i+1]) %>% toJSON()
      formData <- list(
        "token" = tk,
        content = "record",
        action = "import",
        format = "json",
        type = "flat",
        overwriteBehavior = ov,
        forceAutoNumber = fan,
        data = data_to_upload_json,
        returnContent = "count",
        returnFormat = "json"
      )
      response <- POST(urlapi, body = formData, encode = "form")
      result_list[[i]] <- content(response)
      if(is.na(as.numeric(result_list[[i]]))) stop(paste("Error in push: ", result_list[[i]]))
    }
    return(sum(unlist(result_list)))
  }
  
  
}

# upload_data_rc(token_adult, 
#           toJSON(data.frame(record_id = "RA11602-00022",
#                      redcap_event_name = "followup_3_arm_1",
#                      test_ekg_eligdt = "2023-7-14",
#                      test_sixmin_eligdt = "2023-7-14")),
#           ov="overwrite")


retrieve_rc_data <- function(tk, addit_vrb = as.character(), form = NA, recs, 
                             urlapi = "https://redcap.partners.org/redcap/api/", 
                             return_fd = F) {
  form_data <- list(
    "token" = tk,
    content = "record",
    action = "export",
    format = "csv",
    type = "flat",
    csvDelimiter = "",
    rawOrLabel = "raw",
    rawOrLabelHeaders = "raw",
    exportCheckboxLabel = "false",
    exportSurveyFields = "false",
    exportDataAccessGroups = "false",
    returnFormat = "csv"
  )
  
  
  if(!is.na(form)) {
    for(i in 0:(length(form) - 1)){
      form_data[glue("forms[{i}]")] = form[i+1]
    }
    addit_vrb = unique(c("record_id", addit_vrb))
  }
  
  if(length(addit_vrb) > 0) {
    for(i in 0:(length(addit_vrb) - 1)){
      form_data[glue("fields[{i}]")] = addit_vrb[i+1]
    }
    
  } 
  if(!missing(recs)) {
    for(i in 0:(length(recs) - 1)) form_data[glue('records[{i}]')] = recs[i+1]
  }
  if(return_fd){
    return(form_data)
  } 
  
  response <- POST(urlapi, body = form_data, encode = "form")
  content_chr(response)
}

### API raw data load functions

get_res <- function(urlapi, body, encode, response, try_count=3){
  if(!missing(response)) {
    if(response$status_code == 200) return(response)
    print("waiting...")
    Sys.sleep(5)
  }
  if(try_count == 0) stop(glue("API Failed while downloading dataset including participant \n{body[[length(body)]]}"))
  response <- httr::POST(urlapi, body=body, encode = encode)
  get_res(urlapi, body, encode, response, try_count - 1)
}


get_loc <- function(rt, loc){
  today_dt <- format(Sys.Date(), "%Y%m%d")
  loc_base <- file.path(rt, loc, "DM_src", today_dt)
  dir.create(loc_base, recursive = T)
  dir.create(glue("{loc_base}/all_forms"))
  dir.create(glue("{loc_base}/all_rfiles"))
  dm_rt_dates_chr <- na.omit(suppressWarnings(as.numeric(list.files(file.path(rt, loc, "DM_src")))))
  dm_rt_dates <- dm_rt_dates_chr[order(dm_rt_dates_chr, decreasing = T)]
  dm_rt_date <- setdiff(dm_rt_dates, format(Sys.Date(), "%Y%m%d"))[1]
  loc_last <- file.path(rt, loc, "DM_src", dm_rt_date)
  list(loc_base=loc_base, loc_last=loc_last)
}

get_byid <- function(all_ids, urlapi, formData, n_batch=20, content_fxn = content_chr, encode="form", verbose=T){
  
  all_ids_seq <- seq(0, length(all_ids), by=n_batch)
  if(length(all_ids) > all_ids_seq[length(all_ids_seq)]) all_ids_seq = c(all_ids_seq,  length(all_ids))
  
  all_ids_cutlist <- lapply(1:(length(all_ids_seq) - 1), function(i) {
    all_ids[(all_ids_seq[i]+1):(all_ids_seq[i+1])]
  })
  
  cat(glue("------------------- starting pull - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  
  if(exists("start_sink") & !verbose) {
    sink()
    sink(type="message")
  }
  
  result_list <- lapply(all_ids_cutlist, function(x) {
    try_count = 3
    named_id_list <- setNames(as.list(x), glue('records[{0:(length(x) - 1)}]'))
    formData_ids <- c(formData, named_id_list)
    response <- get_res(urlapi, body = formData_ids, encode = encode)
    
    content_fxn(response)
  })
  if(exists("start_sink") & !verbose) start_sink()
  return(result_list)
}

get_rc_params <- function(tk, urlapi = "https://redcap.partners.org/redcap/api/"){
  
  pi_formData <- list("token"=tk,
                      content='project',
                      format='csv',
                      returnFormat='csv')
  
  pi_response <- httr::POST(urlapi, body = pi_formData, encode = "form")
  
  pi_res <- httr::content(pi_response)
  
  rv_formData <- list("token"=tk,
                      content='version')
  
  rv_response <- httr::POST(urlapi, body = rv_formData, encode = "form")
  rv_res <- httr::content(rv_response)
  
  rc_version <- paste0("redcap_v", names(rv_res))
  pid <- pi_res$project_id
  list(rc_version=rc_version, 
       pid=pid,
       url=gsub("api.+", "", urlapi),
       urlapi = urlapi)
}

get_rc_formdata <- function(tk, loc_head, urlapi, ret=F){
  
  ## REDCap survey queue download should be added when available
  
  meta_list <- list()
  cat(glue("------------------- reading data - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  formData_dd <- list("token" = tk,
                      content = 'metadata',
                      format = 'csv',
                      rawOrLabelHeaders = 'raw',
                      returnFormat = 'csv'
  )
  response_dd <- httr::POST(urlapi, body = formData_dd, encode = "form")
  meta_list$result_dd <- content_chr(response_dd) 
  
  
  checkbox_options <- meta_list$result_dd %>%
    filter(field_type == "checkbox") %>%
    select(field_name, select_choices_or_calculations) %>%
    mutate(choice = str_split(select_choices_or_calculations, "\\s?\\|\\s?")) %>%
    unnest(choice) %>%
    mutate(lev = gsub("^-", "_", str_extract(choice, "^[^,]+(?=,)")),
           vr_name_checkbox = paste(field_name, lev, sep="___"))
  
  dd_form_vrs <- meta_list$result_dd %>%
    left_join(checkbox_options, by = c("field_name")) %>%
    mutate(across(field_name, \(x) coalesce(vr_name_checkbox, field_name))) %>%
    select(field_name, form_name)
  
  kys <- c("record_id", "redcap_event_name", "redcap_repeat_instrument", "redcap_repeat_instance")
  
  
  meta_list$proj <- get_rc_params(tk, urlapi)
  
  pid <- meta_list$proj$pid
  
  fl_prefix = glue("{toupper(loc_head)}_PID{pid}")

  write.csv(meta_list$result_dd, glue("{loc_list[[loc_head]]$loc_base}/{fl_prefix}_DataDictionary_{today_tm}.csv"), row.names=F)
  dt_list_full <- unique(dd_form_vrs$form_name)
  
  
  cat(glue("------------------- data dictionary out - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  
  cat(glue("------------------- Downloading all forms - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  form_list <- nlapply(dt_list_full, \(x) {
    form_ds_raw <- retrieve_rc_data(tk, addit_vrb = "record_id", form = x, urlapi=urlapi)
    form_ds <- form_ds_raw %>% 
      mutate(cnt_nan = rowSums(!is.na(pick(-any_of(kys))))) %>% 
      filter(cnt_nan > 0) %>% 
      select(-cnt_nan) 
    
    form_ds %>% 
      write.csv(glue("{loc_list[[loc_head]]$loc_base}/all_forms/{fl_prefix}_DATA_{x}_{today_tm}.csv"), row.names=F)
    form_ds %>% 
      qs_save(glue("{loc_list[[loc_head]]$loc_base}/all_rfiles/{fl_prefix}_DATA_{x}_{today_tm}.qs2"))
    cat(glue("------------------- {which(x == dt_list_full)}/{length(dt_list_full)} - {x} - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
    form_ds
  })
  
  
  
  cat(glue("------------------- downloading eventmap - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  formData_eventmap <- list("token"=tk,
                            content='formEventMapping',
                            format='csv',
                            returnFormat='csv')
  api_eventmap <- httr::POST(urlapi, body = formData_eventmap, encode = "form")
  meta_list$res_eventmap <- content_chr(api_eventmap)
  write.csv(meta_list$res_eventmap, glue("{loc_list[[loc_head]]$loc_base}/{fl_prefix}_eventmap_{today_tm}.csv"), row.names=F)
  
  
  formData_eventidmap <- list("token"=tk,
                              content='event',
                              format='csv',
                              returnFormat='csv')
  api_eventidmap <- httr::POST(urlapi, body = formData_eventidmap, encode = "form")
  meta_list$res_eventidmap <- content_chr(api_eventidmap)
  write.csv(meta_list$res_eventidmap, glue("{loc_list[[loc_head]]$loc_base}/{fl_prefix}_eventidmap_{today_tm}.csv"), row.names=F)
  
  
  
  
  cat(glue("------------------- downloading queries - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  
  pull_url <- dq_url(urlapi, "export", pid)
  
  fd_queries <- list("token"=tk,
                     format='json',
                     returnFormat='json'
  ) 
  
  res_queries <- get_res(pull_url, body = fd_queries, encode = "form")
  
  res_queries_raw <- content_chr(res_queries)
  
  if(F){
    res_queries_chr <- rawToChar(unlist(res_queries_raw))
    res_queries_json <- fromJSON(res_queries_chr)
    
    cq_ds <- bind_rows(lapply(res_queries_json, function(x){
      # print(x$record)
      res_ds <- bind_rows(lapply(x$resolutions, function(xx) {
        ds <- as.data.frame(t(unlist(xx)))
        names(ds) <- gsub("^[1-9]+.", "", names(ds))
        ds
      }))
      res_loc <- as.data.frame(t(unlist(x[!names(x) == "resolutions"])))
      if(nrow(res_ds) == 0) return(res_loc)
      cbind(res_loc,
            res_ds %>% select(-any_of("status_id")))
    }))
    
    cq_ds %>% 
      left_join(meta_list$res_eventidmap %>% 
                  select(event_id, redcap_event_name = unique_event_name),
                by = join_by(event_id)) %>% 
      write.csv(glue("{loc_list[[loc_head]]$loc_base}/{fl_prefix}_allqueries_{today_tm}.csv"), row.names=F)
  }
  
  
  cat(glue("------------------- downloading dag information - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  formData_dags <- list("token"=tk,
                        content='userDagMapping',
                        format='csv',
                        returnFormat='csv')
  api_dags <- httr::POST(urlapi, body = formData_dags, encode = "form")
  meta_list$res_dags <- content_chr(api_dags)
  write.csv(meta_list$res_dags, glue("{loc_list[[loc_head]]$loc_base}/{fl_prefix}_dagassigns_{today_tm}.csv"), row.names=F)
  
  cat(glue("------------------- downloading user information - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  formData_users <- list("token"=tk,
                         content='user',
                         format='csv',
                         returnFormat='csv')
  api_users <- httr::POST(urlapi, body = formData_users, encode = "form")
  meta_list$res_users <- content_chr(api_users)
  write.csv(meta_list$res_users, glue("{loc_list[[loc_head]]$loc_base}/{fl_prefix}_userassigns_{today_tm}.csv"), row.names=F)
  
  
  cat(glue("------------------- downloading report form info - {format(Sys.time(), '%H:%M')} ------------------- \n\n"))
  formData_repeatforms <- list("token"=tk,
                               content='repeatingFormsEvents',
                               format='csv',
                               returnFormat='csv')
  api_repeatforms <- httr::POST(urlapi, body = formData_repeatforms, encode = "form")
  meta_list$res_repeatforms <- content_chr(api_repeatforms)
  write.csv(meta_list$res_repeatforms, glue("{loc_list[[loc_head]]$loc_base}/{fl_prefix}_repeatforms_{today_tm}.csv"), row.names=F)
  
  formData_xml <- list("token"=tk,
                       content='project_xml',
                       returnMetadataOnly='true',
                       format = "xml",
                       returnFormat = "xml")
  
  response_xml <- httr::POST(urlapi, body = formData_xml, encode="form")
  result_xml <- content(response_xml)
  
  xml_as_list <- xml2::as_list(result_xml)
  
  sq_list <- tryCatch(xml_as_list$ODM$Study$GlobalVariables$SurveysQueueGroup, error=function(e) NULL)
  fd_list <- tryCatch(xml_as_list$ODM$Study$GlobalVariables$FormDisplayLogicConditionsGroup, error=function(e) NULL)
  
  meta_list$survey_queue <- if(length(sq_list) > 0){
    lapply(sq_list, \(x) {
      if(is.null(x)) stop("No survey queue found")
      as_tibble(attributes(x))
    }) %>% 
      bind_rows()
  } else {
    print("No survey queue found, saving blank form")
    data.frame(active = as.character(),
               auto_start = as.character(),
               condition_surveycomplete_survey_id = as.character(),
               condition_surveycomplete_event_id = as.character(),
               condition_andor = as.character(),
               condition_logic = as.character(),
               event_id = as.character(),
               survey_id = as.character(), 
               stringsAsFactors=FALSE) 
  }
  
  meta_list$fd_logic <- if(length(fd_list) > 0){
    lapply(fd_list, \(x) {
      as_tibble(attributes(x))
    }) %>% 
      bind_rows() %>% 
      separate_longer_delim(forms_events, ",") %>% 
      separate_wider_delim(forms_events, ":", names=c("event_name", "form_name"))
  } else {
    print("No form display logic found, saving blank form")
    data.frame(control_condition = as.character(),
               event_name = as.character(),
               form_name = as.character(), 
               stringsAsFactors=FALSE) 
  }
  
  write.csv(meta_list$survey_queue, glue("{loc_list[[loc_head]]$loc_base}/{fl_prefix}_surveyqueue_{today_tm}.csv"), row.names=F)
  write.csv(meta_list$fd_logic, glue("{loc_list[[loc_head]]$loc_base}/{fl_prefix}_formdisplaylogic_{today_tm}.csv"), row.names=F)
  
  print(glue("Files saved to {loc_list[[loc_head]]$loc_base}"))
  
  if(ret) list(form_list = form_list, cq_ds=cq_ds, meta_list=meta_list)
}





gen_link <- function(rc_version, pid, redcap_repeat_instance=1, event_id, record_id, form, variable='', tk,
                     urlrc = "https://redcap.partners.org/redcap", linklbl = "REDCap Link") {
  
  if(missing(rc_version) | missing(pid)){
    if(missing(tk)) stop("Function needs either version and pid, or a token")
    param_list <- get_rc_params(tk, urlapi = paste0(urlrc, "/api/"))
    rc_version = param_list$rc_version
    pid= param_list$pid
  }
  if(missing(event_id)){
    url_out <- glue("{urlrc}/{rc_version}/DataEntry/record_home.php?pid={pid}&arm=1&id={record_id}")
    urlhtml_out <- glue("<a href='{url_out}' target='_blank'>{linklbl}</a>")
    return(list(link=url_out, url=urlhtml_out))
  }
  
  url_out <- glue("{urlrc}/{rc_version}/DataEntry/index.php?pid={pid}&instance={redcap_repeat_instance}&event_id={event_id}&id={record_id}&page={form}&fldfocus={variable}#{variable}-tr")
  urlhtml_out <- glue("<a href='{url_out}' target='_blank'>{linklbl}</a>")
  list(link=url_out, url=urlhtml_out)
}

gen_link_ds <- function(ds, tk, dd, evnt, 
                        nm_instance = "redcap_repeat_instance", 
                        nm_revent = "redcap_event_name", 
                        nm_event = "event_id", 
                        nm_fm = "form_name", 
                        nm_vr = "field_name", 
                        nm_rec = "record_id",
                        focus=T) {
  # evnt <- adult_env_list$ds_eventmap_id()
  # dd <- adult_env_list$ds_dd()
  
  if(nm_instance != "redcap_repeat_instance" & "redcap_repeat_instance" %in% names(ds)) ds <- rename(ds, redcap_repeat_instance_delglds = redcap_repeat_instance)
  
  if(nm_instance %!in% names(ds)){
    ds[[nm_instance]] = 1
  }
  
  
  if(nm_event %!in% names(ds)){
    ds <- ds %>% 
      left_join(evnt, by=join_by(!!sym(nm_revent) == redcap_event_name))
  }
  
  if(nm_fm %!in% names(ds)){
    ds <- ds %>% 
      left_join(dd %>% select(field_name, form_name),
                by=join_by(!!sym(nm_vr)  == field_name))
  }
  
  param_list <- get_rc_params(tk)
  rc_version = param_list$rc_version
  pid= param_list$pid
  
  ds_links <- ds %>% 
    rename(redcap_repeat_instance = !!sym(nm_instance),
           event_id = !!sym(nm_event),
           record_id = !!sym(nm_rec),
           form_name = !!sym(nm_fm),
           field_name = !!sym(nm_vr)) %>% 
    mutate(url_chk_focus = ifelse(rep(focus, n()), glue("&fldfocus={field_name}#{field_name}-tr"), ""),
           url_out = glue("{param_list$url}/{rc_version}/DataEntry/index.php?pid={pid}&instance={redcap_repeat_instance}&event_id={event_id}&id={record_id}&page={form_name}{url_chk_focus}"),
           urlhtml_out = glue("<a href='{url_out}' target='_blank'>REDCap Link</a>"))
  
  ds %>% 
    mutate(url_out = ds_links$url_out,
           urlhtml_out = ds_links$urlhtml_out)
}

open_log_fxn <- function(coh_params, id, st_dt = as.Date("2021-10-1"), end_dt = Sys.Date(), rc_url="https://recover-redcap.partners.org/"){
  glue(rc_url,
       "{coh_params$rc_version}/Logging/index.php?",
       "pid={coh_params$pid}&beginTime={format(st_dt, '%m/%d/%Y')}%2000:01",
       "&endTime={format(end_dt, '%m/%d/%Y')}%2000:01",
       "&usr=&record={id}&logtype=record_edit&dag=")
}


# adult_params <- get_rc_params(token_adult)
# open_log_fxn(adult_params, "RA1S002-00002", st_dt=Sys.Date() - 7)

## form_completeness functions ------

# functions for automated data queries generated using the data dictionary
# sourced by DQ_run.R


# check for missing and expected variables at individual variable level (checkboxes counted as 1 variable missing if nothing checked)
# if checking fversion first before querying, add this to front of val_txt: "(!('{form_name}' %in% dd_list$form_fversions$form_name) || !na_or_blank(eval(parse(text = filter(dd_list$form_fversions, form_name == '{form_name}')$field_name)))) & "
complete_cfxn <- function(ddv = dd_list[["dd_val"]]) {
  
  ddv %>%
    mutate(
      val_txt = case_when(
        field_type == "checkbox" ~ glue("!({any_checked})"),
        T ~ glue("na_or_blank({field_name})")
      ),
      qry_txt = glue("{field_name} missing or blank"),
      qrycond_txt = ""
    ) %>% 
    select(form = form_name, vr= field_name, val_txt, qry_txt, qrycond_txt) %>% 
    mutate(check = "missing_check")
}

# check for out of range values
val_cfxn <- function(ddv, today_dt) {
  ddv %>% 
    filter(!na_or_blank(text_validation_min) | !na_or_blank(text_validation_max)) %>% 
    mutate(
      val_txt_lo = case_when(
        text_validation_min == "today" ~ glue("FALSE"), ## ignore "today"
        text_validation_type_or_show_slider_number == "date_mdy" & str_detect(text_validation_min, "\\[")  ~ glue("as.Date({field_name}) < as.Date({text_validation_min})") %>% convert_branching_logic(), ## if variable, remove square bracket
        text_validation_type_or_show_slider_number == "date_mdy" ~ glue("as.Date({field_name}) < as.Date('{text_validation_min}')"), 
        T ~ glue("as.numeric({field_name}) < as.numeric('{text_validation_min}')")
      ),
      val_txt_hi = case_when(
        text_validation_max %in% c("today", "[clab_datemax]") ~ glue("as.Date({field_name}) > as.Date('{today_dt}')"), 
        text_validation_max == "now" ~ glue("ymd_hm({field_name}) > Sys.time()"), 
        text_validation_type_or_show_slider_number == "date_mdy" & str_detect(text_validation_max, "\\[")  ~ glue("as.Date({field_name}) > as.Date({text_validation_max})") %>% convert_branching_logic(), ## if variable, remove square bracket
        text_validation_type_or_show_slider_number == "date_mdy" ~ glue("as.Date({field_name}) > as.Date('{text_validation_max}')"),
        T ~ glue("as.numeric({field_name}) > as.numeric('{text_validation_max}')")
      ),
      val_txt = case_when(
        na_or_blank(text_validation_min) ~ val_txt_hi,
        na_or_blank(text_validation_max) ~ val_txt_lo,
        T ~ glue("{val_txt_lo} | {val_txt_hi}")
      ),
      qry_txt = glue("{field_name} out of range"),
      qrycond_txt = case_when(
        na_or_blank(text_validation_min) ~ glue("Max: {text_validation_max}"),
        na_or_blank(text_validation_max) ~ glue("Min: {text_validation_min}"),
        T ~ glue("Range: {text_validation_min}, {text_validation_max}")
      )) %>% 
    select(form = form_name, vr= field_name, val_txt, qry_txt, qrycond_txt) %>%
    mutate(check = "range_check",
           across(val_txt, \(x) gsub("\\(today\\)", "(today_dt)", x)))
}

# for each row in all_dd_chks, evaluate val_txt on ds_fdata
# create query_valtext column for things that were queried (missing or out of range values)
# returns a dataset with columns 


cq_fxn <- function(exp_ds, ds = ds_fdata, cores_max = 5, me=T) {
  
  kys <- c("record_id", "redcap_event_name", "redcap_repeat_instrument",  "redcap_repeat_instance")
  # get data frame with all val_txt for each variable
  all_dd_chks <- exp_ds
  
  mk_val <- function(subds) {
    vr_nm <- subds$vr[1]
    if(vr_nm %in% names(subds)){
      subds %>% mutate(val = paste(!!sym(vr_nm)))
    } else {
      subds %>% mutate(val = "__")
    }
  }
  
  multiselect_nm <- names(ds)[str_detect(names(ds), "___")]
  all_vr <- names(ds)
  all_dd_chks_dt <- all_dd_chks %>% 
    ungroup() %>% 
    setDT()
  all_exprs <- rlang::parse_exprs(all_dd_chks$val_txt)
  
  ds_dt <- setDT(ds) 
  
  
  
  full_run_fxn <- function(i){
    all_dd_chks_rw <- all_dd_chks_dt[i,] %>% select(-val_txt)
    
    check_type = all_dd_chks_dt$check[i]
    vt_expr <- all_exprs[[i]]
    multiselect <- !(all_dd_chks_rw$vr %in% names(ds))
    branching_vr <- unlist(str_extract_all(all_dd_chks_dt$val_txt[i], "\\w+"))
    req_vr <- unique(c(c(kys, "base_biosex", 'visit_missed'), ## adding base_biosex to req_vr
                       branching_vr))
    if(multiselect == T){
      vr <- grep(paste0("^", all_dd_chks_rw$vr, "___"), 
                 multiselect_nm, value = T)
      vt_expr_vr = rlang::parse_expr(glue("!({all_dd_chks_rw$any_checked})"))
    } else{
      vr <- as.character(all_dd_chks_rw$vr)
      vt_expr_vr = rlang::parse_expr(glue("na_or_blank({all_dd_chks_rw$vr})"))
    }
    
    if(!me) {
      vt_expr_vr = T
    }
    
    ds_out <- tryCatch(ds_dt %>% 
                         select(any_of(unique(c(req_vr, vr)))) %>% 
                         mutate(qrc00_exp = !!vt_expr,
                                qrc00_mis = !!vt_expr_vr) %>% 
                         filter(qrc00_exp | !qrc00_mis) %>% 
                         mutate(val = ifelse(rep(multiselect == T, n()), 
                                             "_multiselect_",
                                             as.character(!!sym(all_dd_chks_rw$vr)))) %>% 
                         select(any_of(c(kys)), val, qrc00_exp, qrc00_mis) %>% 
                         mutate(vr = all_dd_chks_rw$vr),
                       error=function(e) {
                         print(paste(all_dd_chks_rw$vr, "-", check_type,  ": ", all_dd_chks_dt$val_txt[i], "   ", i, "   ", "Failed\n\n\n"))
                         data.frame()
                       })
    return(ds_out)
  }
  
  if(cores_max < 10){
    long_ds_chks_list <- lapply(1:nrow(all_dd_chks_dt), full_run_fxn)
    # long_ds_chks_list <- collapse::rapply2d(as.list(1:nrow(all_dd_chks_dt)), full_run_fxn)
    
  } else {
    long_ds_chks_list <- parallel::mclapply(1:nrow(all_dd_chks_dt), full_run_fxn,
                                            mc.cores = min(cores_max, max(1, parallel::detectCores()-10)), mc.allow.recursive = TRUE)
  }
  
  long_ds_chks <- collapse::rowbind(long_ds_chks_list) %>% 
    left_join(all_dd_chks_dt %>% 
                select(vr, form, qry_txt, qrycond_txt),
              by = join_by(vr)) %>% 
    mutate(query_valtxt = ifelse(qrc00_mis %in% T, qrycond_txt, as.character(NA)),
           query = ifelse(qrc00_mis %in% T, qry_txt, as.character(NA)),
           complete = qrc00_mis %in% F) %>% 
    select(any_of(kys),
           expected = qrc00_exp, 
           complete,
           query_valtxt,
           query,
           variable = vr,
           form)
  
  long_ds_chks
}

# returns a dataset with a row for each variable that is expected to be filled out,
# and a column for whether it is actually complete
expect_complete_cfxn <- function(ddv = dd_list[["dd_val"]], ds = ds_fdata, cores_max = 5) {
  
  # review if this breaks everything
  ne_na_fix <- function(x) {
    str_replace_all(x, "!=", "%!in%") %>% 
      str_replace_all("\r|\t", " ")
  }
  
  expected_chks <- ddv %>%
    mutate(
      val_txt = glue("({branching_with_form})"),
      qry_txt = glue("Data expected. Can data be filled in?"),
      qrycond_txt = "Data expected. Can data be filled in?"
    ) %>% 
    select(form = form_name, vr = field_name, val_txt, qry_txt, qrycond_txt, any_checked, field_type) %>% 
    mutate(check = "expected_check",
           across(val_txt, ne_na_fix))
  
  qry_out <- cq_fxn(expected_chks, ds = ds, cores_max = cores_max) %>% 
    arrange(record_id, form)
  
  if(nrow(qry_out) == 0) return(qry_out %>% mutate(complete=as.logical(), expected=as.logical()))
  
  qry_out
}



#ds_to_table functions

# removes all new line characters (helpful for latex footnotes)
rm_new_line <- function(str) {
  gsub("\n", "", str)
}

# Note that escaping ampersand can cause problems if done too late. 
# If you add in an ampersand to mark a new column then escaping will remove the new column mark
# function for preparing text for latex (special characters, etc.)
latex_escape_fxn <- function(x, incl_amp=T, cell_data=F) {
  str_out <- str_replace_all(as.character(x), "_", "$\\\\_$") %>% 
    str_replace_all("#", "\\\\#") %>% 
    str_replace_all("\\n", " \\\\\\\\ ") %>% 
    str_replace_all("%", "\\\\%") %>% 
    str_replace_all("<=", "$\\\\leq$") %>% 
    str_replace_all(">=", "$\\\\geq$") %>% 
    str_replace_all("<", "\\\\textless") %>% 
    str_replace_all(">", "\\\\textgreater")  %>% 
    str_replace_all("\U2013", "$-$") 
  if(incl_amp) str_out <- str_replace_all(str_out, "&", "\\\\&")
  if(cell_data) {
    str_out <- str_out %>% 
      str_replace_all("\\[", "{[") %>% str_replace_all("\\]", "]}") %>% 
      str_replace_all("^ +", "\\\\hspace*{1em}")
  }
  
  str_out
}

#catpaste function used to format categorical variables for output in tables
#Inputs:
#n: numeric; count value that should be reported in table cells
#N: numeric; denominator, total value that n will be divided by
#pct: numeric; n/N, percentage that will be reported in table
#expand: logical; TRUE or FALSE; indicates whether output will be reported as n/N (pct%) or n (pct%)
#digits: numeric; how many digits pct should be rounded to
#Required to input either N or pct, not both
#Output: character string either "n/N (pct%)" or "n (pct%)"
catpaste <- function(n, N=NULL, pct=NULL, expand=FALSE, digits=1) {
  if (is.null(pct) & expand==FALSE) {
    return(paste0(n, ' (', sprintf(n/N*100, fmt = glue("%.{digits}f")), '%)'))
  }
  else if (is.null(pct) & expand==TRUE) {
    return(paste0(n, '/', N, ' (', sprintf(n/N*100, fmt = glue("%.{digits}f")), '%)'))
  }
  else if (is.null(N) & expand==FALSE) {
    return(paste0(n, ' (', sprintf(pct, fmt = glue("%.{digits}f")), '%)'))
  }
  else if (is.null(N) & expand==TRUE) {
    return(paste0(n, '/', N, ' (', sprintf(pct, fmt = glue("%.{digits}f")), '%)'))
  }
}



#ds_to_table function used to transform datasets into tables for reporting
#Input datasets should be just a catpaste and a pivot away from final table 
#(i.e. counts should already be computed for categorical variables)
#Inputs:
#data: data frame or tibble
#colmat: data frame with 3 columns; 
#1) n.cols; names of columns that contain numerators for each cell
#2) N.cols; names of columns that contain denominators for each cell
#each n.cols value will be divided by corresponding N.cols value
#3) col.names; names of columns that will summarize the categorical variables (these will not all show up in final table if grp.var!=NULL)
#if (3) contains 'Overall', an 'Overall' column will be included on far right side of output table
#output.cols: either a character string or a vector of strings;
#names of other columns from original dataset that will be included in output
#digits: numeric; number of digits that percentages will be rounded to
#grp.var: character; if this is provided, resulting table will be pivoted wider using this variable
#arrange.var: character; if this is provided, resulting table rows will be ordered by this variable
#fill.missing: sting to fill in missing cells with
#Output: data frame with columns output.cols and col.names, 
#pivoted so these are repeated for each value of grp.var, if provided, 
#and rows arranged by arrange.var, if provided
ds_to_table <- function(data, colmat, output.cols=NULL, expand=FALSE, grp.var=NULL, 
                        arrange.var=NULL, headn.var=NULL, test_loc) {
  
  if(nrow(data)==0) return(as.data.frame(list('Currently no data'), col.names=' '))
  data <- as.data.frame(data)
  
  if(!is.null(grp.var)) suffixes <- unique(data[[grp.var]])
  
  n.cols <- colmat$n.cols
  N.cols <- colmat$N.cols
  col.names <- colmat$col.names
  digits <- colmat$digits
  
  if (is.null(output.cols)) output.cols <- setdiff(colnames(data), c(n.cols,N.cols,col.names,grp.var))
  
  justn <- colmat$n.cols[colmat$col.names!='Overall']
  noall <- setdiff(n.cols, justn)
  justN <- colmat$N.cols[colmat$col.names!='Overall']
  Noall <- setdiff(N.cols, justN)
  
  if (!is.null(headn.var)) if (justN == headn.var) justN=as.character()
  data <- if (!is.null(grp.var)) {
    data %>%
      complete(!!sym(grp.var), !!sym(output.cols[1])) %>%
      mutate(across(all_of(justn), ~replace(.x, is.na(.x), 0))) %>%
      mutate(across(all_of(Noall), ~replace(.x, is.na(.x), ifelse(all(is.na(.x)), 0, unique(.x[!is.na(.x)]))))) %>%
      group_by(!!sym(grp.var)) %>%
      mutate(across(all_of(headn.var), ~replace(.x, is.na(.x), ifelse(all(is.na(.x)), 0, unique(.x[!is.na(.x)]))))) %>%
      ungroup() %>%
      group_by(!!sym(output.cols[1])) %>%
      mutate(across(all_of(justN), ~replace(.x, is.na(.x), ifelse(all(is.na(.x)), 0, unique(.x[!is.na(.x)])[1])))) %>%
      mutate(across(all_of(noall), ~replace(.x, is.na(.x), ifelse(all(is.na(.x)), 0, unique(.x[!is.na(.x)])[1])))) %>%
      ungroup()
    
  }
  else {
    data %>%
      complete(!!sym(output.cols[1])) %>%
      mutate(across(all_of(justn), ~replace(.x, is.na(.x), 0))) %>%
      mutate(across(all_of(N.cols), ~replace(.x, is.na(.x), unique(.x[!is.na(.x)])[1]))) %>%
      ungroup() %>%
      group_by(!!sym(output.cols[1])) %>%
      mutate(across(all_of(noall), ~replace(.x, is.na(.x), unique(.x[!is.na(.x)])[1]))) %>%
      ungroup()
    
  }
  for (i in 1:length(n.cols)) {
    n <- data[,n.cols[i]]
    N <- data[,N.cols[i]]
    data <- as.data.frame(cbind(data, mapply(catpaste, n=n, N=N, expand=expand, digits=digits[i])))
    colnames(data)[ncol(data)] <- col.names[i]
  }
  if (!is.null(headn.var)) {
    data <- data %>%
      mutate(!!sym(grp.var):=paste0(.data[[grp.var]], '\n(n=',.data[[headn.var]], ')'))
    oN <- ifelse ('Overall' %in% col.names,
                  unique(data[,N.cols][which(col.names=='Overall')]), '')
  } 
  if (!is.null(grp.var) & !is.null(arrange.var)) {
    if('Overall' %in% col.names) {
      data <- data %>%
        select(all_of(output.cols), .data[[grp.var]], all_of(col.names)) %>%
        pivot_wider(names_from=.data[[grp.var]], values_from=c(output.cols[-1],
                                                               col.names[-which(col.names=='Overall')])) %>%
        arrange(as.character(.data[[arrange.var]]))
    }
    else {
      data <- data %>%
        select(all_of(output.cols), .data[[grp.var]], all_of(col.names)) %>%
        pivot_wider(names_from=.data[[grp.var]], values_from=c(output.cols[-1], !!(col.names))) %>%
        arrange(as.character(.data[[arrange.var]]))
    }
    
    names_to_order <- names(data)[unlist(lapply(paste0('_', suffixes), grep, x=names(data)))]
    names_id <- setdiff(names(data), names_to_order)
    data <- data %>%
      select(all_of(names_id), names_to_order)
  }
  else if (!is.null(grp.var) & is.null(arrange.var)) {
    if('Overall' %in% col.names) {
      data <- data %>%
        select(all_of(output.cols), .data[[grp.var]], all_of(col.names)) %>%
        pivot_wider(names_from=.data[[grp.var]], values_from=c(output.cols[-1],
                                                               col.names[-which(col.names=='Overall')]))
    }
    else {
      data <- data %>%
        select(all_of(output.cols), .data[[grp.var]], all_of(col.names)) %>%
        pivot_wider(names_from=.data[[grp.var]], values_from=c(output.cols[-1], !!(col.names)))
    }
    
    names_to_order <- names(data)[unlist(lapply(paste0('_', suffixes), grep, x=names(data)))]
    names_id <- setdiff(names(data), names_to_order)
    data <- data %>%
      select(names_id, names_to_order)
  }
  else if (is.null(grp.var) & !is.null(arrange.var)) {
    data <- data %>%
      select(all_of(output.cols), all_of(col.names)) %>%
      arrange(as.character(.data[[arrange.var]]))
  }
  else {
    data <- data %>%
      select(all_of(output.cols), all_of(col.names))
  }
  if ('Overall' %in% col.names) {
    data <- data %>%
      select(-Overall, Overall)
    colnames(data)[colnames(data)=='Overall'] <- ifelse(!is.null(headn.var), paste0('Overall\n(n=', oN, ')'), 
                                                        'Overall')
  }
  
  # If a row is missing then this should be printed as such and left out of counts.
  # Move to summ_tab negates the need for this
  # data <- data %>%
  #   mutate(across(-1, ~ case_when(
  #     is.na(!!sym(output.cols)) ~ gsub(" \\(.+\\)", "", .x),
  #     T ~ .x
  #   ))) %>%
  #   mutate(across(-1, ~ case_when(
  #     is.na(!!sym(output.cols)) ~ replace(.x, is.na(.x), 0), 
  #     T ~ .x
  #   )))
  if(!missing(test_loc)) {
    all_fls <- list.files(test_loc)
    if(length(all_fls) == 0) {
      write.csv(data, file.path(test_loc, "test_tab_1_dst.csv"), row.names = F)
    } else {
      regexd_n <- max(as.numeric(gsub('_.*.csv', '', gsub('test_tab_','',all_fls))))
      # annoying regex to get number
      write.csv(data, glue("{test_loc}/test_tab_{regexd_n+1}_dst.csv"), row.names = F)
    }
  }
  
  
  return(data)
}

#add_labels function from vr_labels adapted to remove [no label]
add_labels2 <- function(ds, vr_colnm = "vr") {
  ds %>% 
    left_join(vr_labels, by=vr_colnm) %>% 
    mutate(across(vrlabel, ~ ifelse(is.na(vrlabel), vr, vrlabel)))
}


# splitting out summary df from output
summ_df <-  function(ds, vrs, grp=as.character(), vrnm = "Characteristic", overall=T, headn=F, 
                     digits=0, denom=F, test_loc, rm_levs = as.character(c()), cts_summ = "mean"){
  
  dsg <- if(length(grp) == 0){
    ds %>% ungroup()
  } else {
    
    if(overall) {
      dsg_o <- bind_rows(ds, ds %>% mutate(!!sym(grp[length(grp)]) := as.factor("Overall")))
    } else {
      dsg_o <- ds
    }
    
    dsg_o %>%
      #mutate(across(c(!!!syms(grp)), as.character)) %>% 
      group_by(!!!syms(grp))
  }
  na_str <- '0 (0%)' # this doesn't really cut it since denominator needs to be included and later on when used it doesn't know the denom
  na_miss_str <- '0'
  if(cts_summ == "mean") {
    cts_fxn <- function(x) {
      ct_str <- glue("{round(mean(x, na.rm=T), digits=digits)} ({round(sd(x, na.rm=T), digits=digits)})")
      tribble(
        ~level, ~val,
        "Mean (SD)", ct_str, 
        "Missing", glue("{sum(is.na(x))}")
      ) %>% 
        mutate(tot_cnt=sum(!is.na(x))) %>% 
        list()
    }
  }
  if(cts_summ == "median") {
    cts_fxn <- function(x) {
      ct_str <- glue("{round(median(x, na.rm=T))} ({round(quantile(x, 0.25, na.rm = T))}, {round(quantile(x, 0.75, na.rm = T))})")
      tribble(
        ~level, ~val, 
        "Median (Q1, Q3)", ct_str, 
        "Missing", glue("{sum(is.na(x))}")
      ) %>%
        mutate(tot_cnt = sum(!is.na(x))) %>%
        list()
    }
  }
  
  fxn_npct <- function(num, denom, zero_rep="0", null_rep="NA"){
    case_when(
      denom == 0 ~ null_rep,
      num == 0 ~ zero_rep,
      T ~ sprintf("%g/%g (%1.0f%%)", num, denom, 100*num/denom)
    )
  }
  
  cat_fxn <- function(x) {
    levs <- if("factor" %in% class(x)){
      levels(x)
    } else {
      na.omit(unique(x))
    }
    
    glue_str_fxn <- function(num, denom) sprintf("%g (%1.*f%%)", num, digits, 100*num/denom)
    if(denom) glue_str <- glue_str_fxn <- function(num, denom) sprintf("%g/%g (%1.*f%%)", num, denom, digits, 100*num/denom)
    
    if(denom) na_str <- '0/{denom[1]} (0%)'
    
    ds_tot_all <- tibble(level=levs) %>% 
      mutate(tot_cnt = sum(x %!in% c(NA, rm_levs)))
    
    miss_str <- ifelse("Missing" %in% x, "Missing_na", "Missing")
    
    ds_out <- tibble(level = x) %>% 
      group_by(level) %>% 
      summarise(num_val=n(), 
                .groups="drop") %>% 
      ungroup() %>% 
      full_join(ds_tot_all, by = "level") %>% 
      mutate(across(level,  ~ factor(ifelse(na_or_blank(.x), miss_str, as.character(.x)), c(levs, miss_str)))) %>% 
      mutate(across(num_val, ~ ifelse(is.na(.x), 0, .x)),
             val = case_when(
               level %in% c(rm_levs, miss_str) ~ paste(num_val),
               T ~ glue_str_fxn(num_val, tot_cnt))) %>% 
      arrange(level) 
    list(ds_out)
  }
  
  not.numeric <- function(x) !is.numeric(x)
  
  ds_out_cat_list <- dsg %>% 
    select(all_of(c(vrs, grp))) %>% 
    summarise(across(where(not.numeric), cat_fxn),
              across(where(is.numeric), cts_fxn)) 
  
  ds_out_cat <- lapply(1:nrow(ds_out_cat_list), function(i) {
    bind_rows(lapply(setdiff(names(ds_out_cat_list), grp), function(x) {
      ds_out_cat1 <- ds_out_cat_list[i, ]
      if(!"list" %in% class(ds_out_cat1)) ds_out_cat1 <- ds_out_cat1[[x]]
      as.data.frame(ds_out_cat1[[1]]) %>% mutate(vr = x)
    })) %>% 
      bind_cols(ds_out_cat_list[i, grp])
  }) %>% bind_rows() 
  if (length(grp)>0) {
    temp_split_str <- "<<<<<II>>>>>"
    get_el <- function(x, splt, i){
      unlist(lapply(strsplit(as.character(x), splt), function(x) {
        if(length(x) < i) return(as.character(NA))
        x[[i]]
      }))
    }
    all_vr_levs <- unique(apply(ds_out_cat[c("vr", "level")], 1, paste, collapse=temp_split_str))
    ds_out_cat <- ds_out_cat %>%
      mutate(vrlevel=factor(paste0(vr, temp_split_str, level), all_vr_levs)) %>%
      complete(!!sym(grp), vrlevel) %>%
      mutate(level=get_el(vrlevel, temp_split_str, 2), 
             vr=get_el(vrlevel, temp_split_str, 1)) %>%
      group_by(vr) %>%
      mutate(val= case_when(
        is.na(val) & level == "Missing" ~ as.character(na_miss_str), 
        is.na(val) ~ as.character(na_str),
        T ~ as.character(val))) %>%
      select(-vrlevel) 
  }
  
  ds_out_cat
}

summ_tab <- function(ds, vrs, grp=as.character(), vrnm = "Characteristic", overall=T, headn=F, 
                     digits=0, denom=F, test_loc, rm_levs= as.character(c()),
                     outlist=F, cts_summ = "mean", cat_fxn){
  
  if(nrow(ds)==0) {
    if(outlist) { 
      return(list(ds_out=as.data.frame(list('Currently no data'), col.names=' '), 
                  ds_out_wide=as.data.frame(list('Currently no data'), col.names=' ')))
    } else {
      return(as.data.frame(list('Currently no data'), col.names=' '))  
    }
  }
  if(length(grp) == 0) {
    if("temp_summ_tab_grp" %in% names(ds)) stop("Why is temp_summ_tab_grp already a variable?")
    grp = "temp_summ_tab_grp"
    ds <- ds %>%
      mutate(!!sym(grp) := as.factor("temp_summ_tab_grp"))
  }
  if (any(is.na(ds[[grp]]))) stop("Error: missing values in grp variable")
  
  if (!("factor" %in% class(ds[[grp]]))) {
    ds <- ds %>%
      mutate(!!sym(grp) := as.factor(!!sym(grp)))
  }
  if(overall) levels(ds[[grp]]) <- c(levels(ds[[grp]]), 'Overall')
  
  ds_out_raw <- summ_df(ds, vrs, grp, vrnm, overall, headn, digits, denom, test_loc, rm_levs, cts_summ) %>% 
    ungroup() %>% 
    mutate(across(vr, \(x) factor(x, levels=vrs)),
           across(level, factor)) %>% 
    arrange(vr, !!!syms(grp)) %>% 
    add_labels2() 
  
  ds_out <- ds_out_raw %>%
    select(-any_of(c("num_val", "vr"))) %>% 
    relocate(!!sym(vrnm) := vrlabel, !!(grp), Level=level, Value=val)
  
  if(length(grp) == 0) return(ds_out)
  grps <- ds[[grp[length(grp)]]]
  ugrps <- if("factor" %in% class(grps)) {
    levels(grps)
  } else {
    grp_levs <- unique(grps)
    grp_levs[order(grp_levs)]
  }
  #if(overall) ugrps = c(ugrps, "Overall")
  if(headn) {
    
    groupN <- ds %>%
      group_by(!!sym(grp)) %>%
      summarize(n=n()) %>%
      complete(!!sym(grp), fill=list(n=0)) %>%
      mutate(newlab = mapply(paste0, !!sym(grp), '\n(n=', n, ')')) %>%
      select(-n) %>%
      mutate(!!sym(grp) := as.character(!!sym(grp))) %>% 
      na.omit()
    new.ugrps <- unique(groupN$newlab)
    if(overall) {
      groupN <- groupN %>% filter(!!sym(grp)!='Overall')
      overallN <- nrow(ds)
      groupN <- rbind(groupN, c('Overall', paste0('Overall\n(n=', overallN, ')')))
      new.ugrps <- unique(groupN$newlab)
    }
    
    ds_out_wide <- setNames(data.frame(ugrps), grp[length(grp)]) %>% 
      left_join(ds_out %>% select(-any_of("tot_cnt")), by = grp) %>% 
      left_join(groupN, by = grp) %>%
      select(-!!sym(grp)) %>%
      pivot_wider(names_from=newlab, values_from=c(Value)) %>% 
      mutate(across(all_of(new.ugrps), ~ ifelse(is.na(.x), "", .x)))
    
    if(length(vrs) == 1) ds_out_wide = ds_out_wide[-1] %>% rename(!!sym(vrs) := Level)
    if(grp == "temp_summ_tab_grp"){
      ds_out_wide <- ds_out_wide %>% select(-starts_with("temp_summ_tab_grp"))
    }
    
    if(!missing(test_loc)) {
      all_fls <- list.files(test_loc)
      if(length(all_fls) == 0) {
        write.csv(ds_out_wide, file.path(test_loc, "test_tab_1_st.csv"), row.names = F)
      } else {
        regexd_n <- max(as.numeric(gsub('_.*.csv', '', gsub('test_tab_','',all_fls[length(all_fls)]))))
        write.csv(ds_out_wide, glue("{test_loc}/test_tab_{regexd_n+1}_st.csv"), row.names = F)
      }
    }
  } else {
    
    ds_out_wide <- setNames(data.frame(ugrps), grp[length(grp)]) %>% 
      left_join(ds_out %>% select(-any_of("tot_cnt")), by=grp) %>% 
      pivot_wider(names_from=grp[length(grp)], values_from=c(Value)) %>% 
      mutate(across(all_of(ugrps), ~ ifelse(is.na(.x), "", .x)))
    
    if(length(vrs) == 1) ds_out_wide = ds_out_wide[-1] %>% rename(!!sym(vrs) := Level)
    
    if(!missing(test_loc)) {
      all_fls <- list.files(test_loc)
      if(length(all_fls) == 0) {
        write.csv(ds_out_wide, file.path(test_loc, "test_tab_1_st.csv"), row.names = F)
      } else {
        regexd_n <- max(as.numeric(gsub('_.*.csv', '', gsub('test_tab_','',all_fls[length(all_fls)]))))
        write.csv(ds_out_wide, glue("{test_loc}/test_tab_{regexd_n+1}_st.csv"), row.names = F)
      }
    }
    
    if(grp == "temp_summ_tab_grp"){
      ds_out_wide <- ds_out_wide %>% select(-starts_with("temp_summ_tab_grp"))
    }
  }
  
  if(outlist) return(list(ds_out=ds_out_raw, ds_out_wide=ds_out_wide))
  return(ds_out_wide)
}

#printtabon creates flextable object to print in final OSMB report
#Inputs:
#tab: data frame; output from ds_to_table() or summ_tab()
#this should already have columns in order you want for final table
#col.names: vector of character stings; names of columns in output, if different from input df's column names. If shorter than number of columns in table then will append extra column names assuming provided were sequential colnames starting from left.
#merge.cols: character string; name of column(s) that should be merged vertically
#merge.cols: character string; name of column(s) that should be merged vertically
#groupheader: logical T/F; whether to include a separate header row for group used as grp.var or grp in 
#ds_to_table() or summ_tab(), respectively
#col.percent: logical T/F; indicates whether percentages are column percents and changes footnote accordingly
#all.cat: logical T/F; indicates whether all variables in table are categorical and changes footnote accordingly
#all.cont: logical T/F; indicates whether all variables in table are continuous and changes footnote accordingly
#sub0: logical T/F: indicates whether to replace "0%" with "<1%" in table
# latex_hline_above is a vector of numbers
# hline_above_last is a boolean

#' printtab_tex is called in printtab() to handle outputting latex files from 
#' printtab() function

printtab_tex <- function(tab, latex_flnm, custom.foot, merge.cols=NULL,  merge.cols.green = NULL,
                         latex_hline_above, colspec_tex="", one_switch, l_switch, x_switch, 
                         hline_above_last, vline,spec_pop3=F, col.names, adv_events, pres=F,
                         bold_row, pagebreak_lines=as.numeric()){
  
  names(tab) <- col.names
  
  if(missing(vline)) vline=NULL
  else if(vline) vline = "|"
  else if(!vline) vline = NULL
  
  
  if(!is.null(merge.cols)){
    extra_lines <- !duplicated(tab[merge.cols[1]])
    extra_lines[1] <- F
  } else {
    extra_lines <- rep(F, nrow(tab))
  }
  
  if(spec_pop3) {
    tab <- tab[-1]
    tab <- bind_rows(tab[1:6,], tab[7, ], tab[7:nrow(tab), ])
    merge.cols = NULL
    merge.cols.green = NULL
    extra_lines <- rep(F, nrow(tab))
    extra_lines[7:8] <- T
  }
  
  if(missing(hline_above_last)) hline_above_last = F
  if(!missing(latex_hline_above) & !hline_above_last) extra_lines[latex_hline_above] <- T
  
  if(hline_above_last) extra_lines[nrow(tab)] <- T
  
  to_split_in_two <- c(F, sapply(select(tab, -1), function(x) any(grepl("\\(\\d+%", x))))
  
  colnms_tab <- tibble(colnm= names(tab)) %>% 
    mutate(across(colnm, ~ifelse(.x %in% c("", " "), "~", latex_escape_fxn(.x)))) %>% 
    mutate(across(colnm, ~ ifelse(to_split_in_two, 
                                  paste0("\\n\\SetCell[c=2]{c}{", .x, "} & "), 
                                  paste0("\\n\\SetCell[c=1]{c}{", .x, "}")))) 
  
  mk_merged_row <- function(ds, grp_cols, col=''){
    
    mc_vr = grp_cols[length(grp_cols)]
    ds %>% 
      mutate(mmr_row_cnty= rep(n(), n()),
             mmr_row_cntin = 1:n(),
             .by=all_of(c(grp_cols))) %>% 
      mutate(mc_vr_prefix = paste0("\\SetCell[r=", mmr_row_cnty,
                                   "]{", col,
                                   "} "),
             across(any_of(!!mc_vr), \(x) ifelse(mmr_row_cntin > 1, 
                                                 unlist(lapply(str_extract_all(x, " &"), \(xx) paste(xx, collapse=" "))), 
                                                 paste(mc_vr_prefix, str_replace_all(x, " &", paste0(" &\\", mc_vr_prefix)))))) %>% 
      select(-c(mc_vr_prefix, mmr_row_cntin, mmr_row_cnty))
    
  }
  
  split_checkna <- function(x){
    
    find_parens <- gregexpr('\\(', x)
    lapply(1:length(find_parens), function(i){
      
      n_split = max(find_parens[[i]])
      if(n_split %in% c(NA, -1)) return(paste0(x[i], " & "))
      paste0(substr(x[i], 1, n_split-1), " & ", substr(x[i], n_split, nchar(x[i])))
    }) %>% unlist()
  }
  
  tab_replace <- tab %>%
    ungroup() %>% 
    mutate(across(everything(), \(x) latex_escape_fxn(x, cell_data=T)),
           across(all_of(names(to_split_in_two)[to_split_in_two]), split_checkna))
  
  if(!is.null(merge.cols)) {
    for(mi in length(merge.cols):1){
      tab_replace <- tab_replace %>% 
        mk_merged_row(merge.cols[1:mi], col="gray_z")
    }
  } else if(!is.null(merge.cols.green)){
    for(mi in length(merge.cols.green):1){
      tab_replace <- tab_replace %>% 
        mk_merged_row(merge.cols.green[1:mi])
    }
  }
  
  
  
  if(!is.null(merge.cols[1]) | !is.null(merge.cols.green[1])){
    switch_vctr <- rep(1, nrow(tab_replace))
    top_merge <- c(merge.cols, merge.cols.green)[1]
    grp_switches <- which(tab_replace[[top_merge]] != "")[-1]
    switch_vctr[grp_switches-1] <- 0
  } else {
    switch_vctr <- rep(0, nrow(tab_replace))
  }
  
  # switch_vctr <- rep(0, nrow(tab_replace))
  
  if(!missing(bold_row)) {
    tab_replace[bold_row, ] <- tab_replace[bold_row, ] %>% 
      mutate(across(everything(), ~paste("\\rowstyle{\\bfseries}", gsub("&", "& \\\\rowstyle{\\\\bfseries}", .x))))
  }
  
  names(tab_replace) <- colnms_tab$colnm
  
  ## This should probably always be the last operation on tab_in
  ## spec_pop3 is a very breakable exception
  tab_in_rows <- c(paste0(paste(names(tab_replace), collapse=" & "), "\\\\\\n"),
                   paste0(apply(tab_replace, 1, paste, collapse=" & "), 
                          c(sapply(switch_vctr[-1*length(switch_vctr)], function(stari) ifelse(stari==0, "\\\\\\n", "\\\\*\\n")),
                            "\\n")))
  if(length(pagebreak_lines) > 0){
    tab_in_rows[pagebreak_lines] <- paste("\\pagebreak\\n", tab_in_rows[pagebreak_lines])
  }
  tab_in <- paste(tab_in_rows, 
                  collapse="")
  
  if(spec_pop3) {
    all_rows <- c(paste(names(tab_replace), collapse=" & "),
                  apply(tab_replace, 1, paste, collapse=" & "))
    all_rows[8] <- "\\SetCell[c=11]{l}{\\bf U.S. Census Region/Division:}"
    tab_in <- paste(all_rows, collapse="\\\\\\n")
  }
  
  if(adv_events) {
    top_row <-"\\SetCell[r=8]{l}{\\bf Infected \\& Uninfected}"
    rbind(top_row, tab_in)
  }
  rows <- if(length(merge.cols) > 1 | (length(merge.cols) == 0 & length(merge.cols.green) > 1)) {
    gsub("\\|$", "", paste(rep('Q[m]|', nrow(tab) ), collapse=""))
  } else {
    tibble(xline=  c("", "|")[extra_lines + 1],
           row_def = rep('Q[m]', nrow(tab) )) %>%
      apply(1, paste, collapse="") %>% 
      paste0(collapse = "")
    
  }
  
  
  col_spec <- c("Q[r]", "Q[r]Q[l]")[as.numeric(to_split_in_two) + 1]
  if(!missing(one_switch)) col_spec[one_switch] <- gsub("Q\\[r]", "X[1]", col_spec[one_switch])
  if(!missing(x_switch)) col_spec[x_switch] <- gsub("Q", "X", col_spec[x_switch])
  if(!missing(l_switch)) col_spec[l_switch] <- gsub("r", "l", col_spec[l_switch])
  
  cols <- paste0(col_spec, collapse="|")
  #rep("R{0.06\textwidth} C{0.01\textwidth} L{0.10\textwidth} |", ncol(tab_replace))
  header_style <- ifelse(pres, 
                         "|}, row{odd}={bg=NavyBlue!50!DarkOliveGreen!15}, row{1}={bg=NavyBlue!50!DarkOliveGreen, fg=white, font=\\sffamily}}\\n",
                         "|}, row{odd}={bg=CadetBlue!8}, row{1}={bg=CadetBlue, fg=white, font=\\sffamily}}\\n")
  # The following three lines ensures \n is evaluated
  
  file_str_list <- str_split(latex_flnm, "_|/")[[1]]
  len_f <- length(file_str_list)
  file_ref <- file_str_list[len_f-1]
  file_ref_list <- strsplit(file_ref, "s|o")[[1]]
  incl_header = as.numeric(file_ref_list[2]) > 0
  filename_clean = gsub(".tex$", "",  file_str_list[len_f])
  
  latex_header_info = paste0("caption = {",
                             filename_clean,
                             "},
                         note{} = {",
                             latex_escape_fxn(custom.foot),
                             "},
                         label = {tab:",
                             file_ref,
                             "}")
  
  latex_noheader_info = paste0("entry=none,
                              caption=,
                              label=none
                         ")
  
  tab_type <- ifelse(pres, "tblr", "longtblr")
  
  tab_start_str <- if(pres){
    paste0("\\begin{", tab_type, "}")
  } else {
    paste0( "\\begin{",
            tab_type,
            "}[
                         theme = drctab,
                         ",
            ifelse(incl_header, latex_header_info, latex_noheader_info),
            "]")
  }
  
  latex_string <- paste( tab_start_str,
                         "
                         {colspec={", 
                         # vline,
                         cols, 
                         "}",
                         colspec_tex,
                         ", rowhead=1, rowspec={||Q[m]|", 
                         rows, 
                         header_style,
                         tab_in, 
                         "\\n\\end{",
                         tab_type, 
                         "}", 
                         sep="")
  str_list <- strsplit(latex_string,  "\\\\n(?!ormalsize)", perl=T)
  # for debugging
  #cat(str_list[[1]], sep="\n", file=file.path('./tex100.tex'))
  
  cat(custom.foot, file = gsub("tables/", "footers/", gsub(".tex$", "_footer.tex", latex_flnm)))
  
  cat(str_list[[1]], sep="\n", file = file.path(latex_flnm))
  
}

printtab <- function(tab, col.names=colnames(tab), merge.cols=NULL, merge.cols.green=NULL,
                     groupheader=FALSE, footnote=T, col.percent=T, all.cat=T, all.cont=F, 
                     headrows.var=NULL, zebra=T, custom.foot=as.character(), au = F, 
                     str = "", latex_flnm, sub0 = F, latex_hline_above, colspec_tex="", 
                     one_switch, l_switch, x_switch, hline_above_last, vline, spec_pop3=F, adv_events=F,
                     pres=F, bold_row, color = "#EAF1DD", pagebreak_lines=as.numeric()) {
  
  if(length(col.names) < ncol(tab)) col.names = c(col.names, colnames(tab)[-1*(1:length(col.names))])
  if (dim(tab)[1]==1 & dim(tab)[2]==1) 
    if (tab == as.data.frame(list('Currently no data'), col.names=' ')) {
      col.names=' '
    }
  if (sub0==TRUE) {
    tab <- tab %>%
      mutate(across(everything(), ~sub("(^|\\()(0%)", "\\1<1%", .x))) %>%
      mutate(across(everything(), ~ sub("(^|\\()(0.0%)", "\\1<0.1%", .x)))
  }
  if (groupheader==TRUE) {
    header1 <- col.names
    header2 <- colnames(tab) 
    header2[!grepl(".*_", header2)] <- " "
    header2 <- sub('.*_', '', header2)
  }
  else colnames(tab) <- col.names
  nonlevel.cols <- setdiff(which(col.names!='Level'),1)
  
  tab1 <- tab %>% 
    mutate(across(1, ~ ifelse(na_or_blank(.x), "Missing", as.character(.x))))
  if (!is.null(headrows.var)) {
    tab1 <- tab1 %>%
      as_grouped_data(groups=headrows.var)
    fillj <- which(!is.na(tab1[,headrows.var]))
    tab1 <- tab1 %>%
      mutate(across(everything(), ~as.character(.x))) %>%
      mutate(across(everything(), ~ case_when(!is.na(!!sym(headrows.var)) ~ 
                                                replace(.x, is.na(.x), .data[[headrows.var]][is.na(.x)]), 
                                              T ~ .x))) %>%
      select(-!!sym(headrows.var))
    nonlevel.cols <- setdiff(which(names(tab1)!='Level'),1)
  }
  
  if(!missing(latex_flnm)) printtab_tex(tab, latex_flnm, custom.foot, merge.cols, merge.cols.green,
                                        latex_hline_above=latex_hline_above, colspec_tex=colspec_tex, 
                                        one_switch=one_switch, l_switch=l_switch, 
                                        x_switch=x_switch, hline_above_last=hline_above_last, vline=vline,
                                        spec_pop3=spec_pop3, col.names=col.names, adv_events=adv_events, pres=pres, bold_row = bold_row,
                                        pagebreak_lines=pagebreak_lines)
  
  prtab <- tab1 %>%
    flextable() %>%
    autofit() %>%
    bold(part='header')
  if (zebra) {
    prtab <- prtab %>%
      theme_zebra(even_header = color, even_body = color, 
                  odd_header='transparent', odd_bod='transparent') %>%
      border(border.top=fp_border(color='black'), 
             border.bottom=fp_border(color='black'), 
             part='body')
    if (!is.null(merge.cols) | !is.null(merge.cols.green)) {
      prtab <- prtab %>%
        bg(j=c(merge.cols, merge.cols.green), bg=color, part='body')
    }
  }
  prtab <- prtab %>%
    align(align='left') %>%
    align(j=nonlevel.cols, align='center', part='all')
  if (groupheader==TRUE) {
    prtab <- prtab %>%
      delete_part(part='header') %>%
      add_header_row(header1, top=FALSE) %>%
      add_header_row(header2, top=TRUE) %>%
      merge_h(part='header') %>%
      bold(part='header') %>%
      align(align='left') %>%
      align(j=nonlevel.cols, align='center', part='all')
  }
  if (!is.null(merge.cols) | !is.null(merge.cols.green)) {
    prtab <-prtab %>%
      merge_v(j=(c(merge.cols, merge.cols.green))) %>%
      valign(valign='top') %>%
      valign(j=nonlevel.cols, valign='center', part='body')
  }
  if (!is.null(headrows.var)) {
    prtab <- prtab %>%
      merge_h(i = fillj) %>%
      align(i = fillj, align='center')
    if (zebra) {
      prtab <- prtab %>%
        bg(i=fillj, bg='transparent', part='body') %>%
        bg(i=setdiff(1:nrow(tab1),fillj), bg=color, part='body')
    }
  }
  i = 16.5 # width of the side borders in the word_document output (in centimeters)
  w = i*0.3937 # width of the side borders in the word_document output (in inches)
  auto_widths <- (dim(prtab)$widths + 2)/sum((dim(prtab)$widths + 2))
  prtab <- width(prtab, width = w*auto_widths)
  colrow='row'
  if (col.percent) colrow='column'
  fn.str="Continuous data is in the format \"n=n, mean (SD)\" and categorical data \"n (%)\". All percentages are {colrow} percents excluding participants with missing data."
  if (all.cat) fn.str="Data is in the format \"n (%)\". All percentages are {colrow} percents excluding participants with missing data."
  if (all.cont) fn.str="Data is in the format \"n=n, mean (SD)\". All percentages are {colrow} percents excluding participants with missing data."
  if (au == T) str = "Recruitment of \"Acute Uninfected\" participants has not yet started. \n"
  if (footnote) {
    prtab <- prtab %>%
      add_footer_lines(values = glue(str, fn.str)) %>%
      font(fontname='Arial', part='all') %>%
      padding(padding.top=1, padding.bottom=1)
    if (length(custom.foot) > 0) 
      prtab <- prtab %>%
        add_footer_lines(custom.foot)
    return(prtab)
  }
  prtab <- prtab %>%
    font(fontname='Arial', part='all') %>%
    padding(padding.top=1, padding.bottom=1)
  if (length(custom.foot) > 0) 
    prtab <- prtab %>%
    add_footer_lines(custom.foot)
  return(prtab)
}



#----------------------------------DEMO FUNCTION---------------------------------------

demo_fxn <- function(tab, grp=as.character(), headn=T, denom=T, overall=T, 
                     vrs, vrs_list, incl_missing=T, outlist=F, rm_levs = as.character(c()), cts_summ = "mean") {
  
  if(!missing(vrs)){
    st_vrs <- unlist(lapply(vrs, function(vr) {
      if(vr %in% names(vrs_list)) return(vrs_list[[vr]])
      vr
    }))
  } else {
    st_vrs <- unname(unlist(vrs_list))
  }
  
  res_obj <- summ_tab(tab, st_vrs, grp=grp, headn=headn, denom=denom, overall=overall, outlist=outlist, rm_levs=rm_levs, cts_summ=cts_summ) 
  
  get_notblank <- function(x) {
    x[!x==""]
  }
  if(outlist) {
    res <- res_obj$ds_out_wide
  } else {
    res <- res_obj
  }
  for(grp_vrs_nm in get_notblank(names(vrs_list))){
    grp_vrs <- vrs_list[[grp_vrs_nm]]
    grp_vrs_labs <- setNames(vr_labels$vrlabel, vr_labels$vr)[grp_vrs]
    grp_vrs_labs[is.na(grp_vrs_labs)] <- grp_vrs[is.na(grp_vrs_labs)]
    res <- res %>% 
      filter(!(Characteristic %in% grp_vrs_labs & Level == "XXXNOTXXX"))
    if(incl_missing) {
      res <- res %>% filter(!(Characteristic %in% grp_vrs_labs[-length(grp_vrs_labs)] & Level == "Missing"))
    } else {
      res <- res %>% filter(Level != "Missing")
    }
    
    res <- res %>% mutate(across(Level, ~ ifelse(Characteristic %in% grp_vrs_labs & (is.na(.x) | .x == ""), "Mean (SD)", as.character(.x))))
    res$Characteristic[res$Characteristic %in% grp_vrs_labs] <- grp_vrs_nm
  }
  
  if(outlist) return(c(res_obj, list(summ_ds = ungroup(res))))
  return(ungroup(res))
}
#------------------------------------Transpose Table Function----------------------------

transpose_table <- function(tab) {
  res <- tab %>%
    t() %>%
    as.data.frame()
  colnames(res) <- res[1,]
  res <- res[-1,]
  res$rows <- rownames(res)
  rownames(res) <- NULL
  res.final <- res %>%
    select(rows, all_of(colnames(res)[-length(colnames(res))]))
  return(res.final)
}

comp_tab_fxn <- function(ds, ac, headn = F){
  if(nrow(ds) == 0) return(regulartable(tibble(` `= "Currently No Data"))) 
  if (headn == T) {
    ds %>%
      mutate(form_f = glue("{form_f} (n={enrollN})")) %>%
      ds_to_table(colmat=colmat_bcomp, expand=F) %>% 
      mutate(across(all_of(ac), ~ gsub(" .+", "%", .x))) %>% 
      transpose_table()
  }
  else {
    ds %>%
      ds_to_table(colmat=colmat_bcomp, expand=T) %>% 
      mutate(across(all_of(ac), ~ gsub("/.+", "%", .x))) %>% 
      transpose_table()
  }
}

#### Function for creating new AE table ####

# data needs to have a column called "ae_yn" indicating whether each event is an AE
ae_to_table <- function(vars, ds, id.var, grp = NULL, digits = 1) {
  summ_grp_fxn <- function(ds_in, grp, id.var, x=NULL, digits=0){
    
    ds_in %>%
      mutate(n_tot = length(unique(!!sym(id.var))),
             .by=!!sym(grp)) %>%
      filter(ae_yn == "Yes") %>% 
      summarize(n_event = n(), 
                n = length(unique(!!sym(id.var))), 
                n_tot = unique(n_tot),
                .by=c(!!!syms(c(grp, x)))) %>%
      mutate(pct = sprintf("%1.*f%%", digits, n/n_tot * 100)) %>%
      arrange(!!sym(grp), !!sym(x)) %>% 
      rename(Level = !!sym(x)) %>%
      mutate(vr = x)
  }
  
  vars_nms = setNames(names(vars), vars)
  
  ds_out <- lapply(c("All AEs", vars), \(x) summ_grp_fxn(x, id = id.var, ds_in = ds %>% mutate(`All AEs`="Total"), grp = grp, digits = digits)) %>%
    bind_rows() %>% 
    mutate(vrlabel = coalesce(vars_nms[vr], vr)) %>% 
    select(-vr) %>%
    rename(Characteristic = vrlabel) %>% 
    mutate(across(Level, \(x) replace_na(x, "Missing")))
  
  if(length(grp) > 0) {
    return(ds_out %>%
             pivot_wider(id_cols = c('Characteristic', 'Level'), 
                         names_from = !!sym(grp),
                         values_from = c('n_event', 'n', 'pct'),
                         names_vary="slowest") %>% 
             mutate(across(starts_with("n_"), \(x) replace_na(x, 0)),
                    across(starts_with("pct_"), \(x) replace_na(x, "0.0%"))))
  } else {
    return(ds_out %>% 
             relocate(Characteristic, Level, .before=1) %>% 
             mutate(across(c(n_event, n), \(x) replace_na(x, 0)),
                    across(c(pct), \(x) replace_na(x, "0.0%"))))
  }
  
}

##-----Summary table with two or less by group variables-----##
#generic function:
#this function depends on another function: demo_fxn()
#return a Flextable object that summarize data based on the parameter input:
#a = target data.frame or tibble object
#v_tar = vector containing target variables of interest
#v_by_group = by group variables, please have no more than 2 (default = NA)
#denom = whether to show denominator in percentage calculation or not (default = F)
summary_table = function(a, v_tar, v_by_group = NA, denom = F) {
  a = tibble(a)
  flx_output = NULL
  if (length(v_by_group) > 2) {
    stop("By group is more than 2 variables, resulting table is not predictable!")
  }
  if (length(v_by_group) == 2 & length(v_tar) >= 2) {
    stop(glue("When by group has two or more variables, target variable vector 
    should have only one variabe."))
  }
  if (NA %in% v_by_group) {
    flx_output = demo_fxn(a, vrs_list = v_tar, denom = denom) %>%
      flextable()
  } else if (length(v_by_group) == 1) {
    flx_output = demo_fxn(a, vrs_list = v_tar, grp = v_by_group, denom = denom) %>%
      flextable()
  } else {
    l_by_group = levels(a[[glue("{v_by_group[2]}")]])
    var_sub_width = length(levels(a[[v_by_group[1]]]))
    flx_total = a %>%
      filter(!!sym(v_by_group[2]) %in% l_by_group) %>%
      demo_fxn(vrs_list = v_tar, overall = T, denom = denom) %>%
      pivot_longer(cols = names(.)[names(.) %!in% v_tar]) %>%
      mutate(col_name = paste(name, sep = "."))
    flx_output = lapply(l_by_group, function(x) {
      tbl_out = a %>%
        filter(!!sym(v_by_group[2]) %in% x) %>%
        demo_fxn(vrs_list = v_tar, grp = v_by_group[1], overall = F, denom = denom) %>%
        pivot_longer(cols = names(.)[names(.) %!in% v_tar]) %>%
        mutate(col_name = paste(v_by_group[2], x, name, sep = "."))
    }) %>%
      bind_rows() %>%
      bind_rows(flx_total) %>%
      pivot_wider(id_cols = v_tar, names_from = "col_name", values_from = "value", 
                  names_repair = "minimal") %>%
      flextable() %>%
      span_header(sep = "\\.") %>%
      vline(j = seq(from = 1, to = length(.$body$col_keys), 
                    by = var_sub_width), part = "all") %>%
      border_outer()
  }
  return(flx_output)
}

#####---- Helper functions

OSMB_tex <- function(outloc, rep_name, rep_dt, edition_notice = "", executive_summary = "", OSMB=F, section_titles=data.frame(section=as.numeric()), n_runs=2) {
  rep_dt_chr1 <- format(rep_dt, '%B %d, %Y')
  rep_dt_chr2 <- format(rep_dt, '%Y %B')
  title_page <- paste0("\\documentclass[fontsize=10pt]{article}

\\usepackage[T1]{fontenc}
\\usepackage[usenames,dvipsnames,table]{xcolor}
\\usepackage{graphicx}
\\usepackage{lastpage}
\\usepackage{hyphenat}
\\usepackage{import}
\\usepackage{multirow}
\\usepackage{tabularray}
\\usepackage{chngcntr}
\\usepackage{titlesec}
\\usepackage{fancyvrb,cprotect}
\\renewcommand{\\sfdefault}{ppl} % Palatino
\\fontfamily{ppl}
\\newcommand{\\rowstyle}[1]{\\gdef\\currentrowstyle{#1}%
  #1\\ignorespaces
}

\\usepackage{array,amsmath}
\\usepackage{ragged2e}

\\usepackage{enumitem}
\\usepackage{flafter}

\\setlength\\tabcolsep{0pt}
\\setlength\\tabcolsep{0pt}
\\def\\arraystretch{1.5}

\\setlength\\parindent{0pt}
\\setlength{\\parskip}{1ex}

\\usepackage[margin=.5in, tmargin=.75in]{geometry}
\\usepackage{fancyhdr}

\\usepackage[hidelinks]{hyperref}

\\definecolor{gray_z}{HTML}{EFEFF3}

\\UseTblrLibrary{amsmath,booktabs}

\\lhead{CONFIDENTIAL}
\\rhead{\\footnotesize ", rep_name, "}
         
\\cfoot{Page \\thepage\\ of \\pageref{LastPage}}
\\lfoot{\\footnotesize \\today}
\\rfoot{\\footnotesize Prepared by RECOVER DRC}
\\pagestyle{fancy}  

\\setcounter{section}{1}



\\DefTblrTemplate{conthead-text}{wide}{ (Continued)}
\\SetTblrTemplate{conthead-text}{wide}%

\\DefTblrTemplate{head}{wide}{%
  \\makebox[\\hsize][c]{%
    \\parbox{\\textwidth}{%
    
      \\centering
      \\UseTblrTemplate {caption-tag}{default}%
      \\UseTblrTemplate {caption-sep}{default}%
      \\UseTblrTemplate {caption-text}{default}%
    }
  }
}

\\DefTblrTemplate{contfoot-text}{wide}{Continued on next page}
\\SetTblrTemplate{contfoot-text}{wide}

\\DefTblrTemplate{note}{wide}{%
  \\makebox[\\hsize][c]{%
    \\parbox{\\textwidth}{%
      \\setlength{\\parindent}{0pt}
      \\MapTblrNotes{
         \\noindent
         \\UseTblrTemplate{note-tag}{default}
         \\UseTblrTemplate{note-sep}{default}
         \\UseTblrTemplate{note-text}{default}
         \\par
         }
    }
  }
}


\\NewTblrTheme{drctab}{
 \\SetTblrTemplate{head}{wide}%
 \\SetTblrTemplate{note}{wide}%
 \\SetTblrStyle{caption-tag}{font=\\bfseries\\normalsize}
 \\SetTblrStyle{caption-text}{font=\\normalsize}
 \\SetTblrStyle{foot}{font=\\footnotesize} 

}


\\begin{document}






\\titleformat{\\section}
{\\color{CadetBlue}}{\\thesection}{1em}{}

\\titleformat{\\subsection}
{\\color{CadetBlue}}{\\thesubsection}{1em}{}

\\begin{titlepage}

\\centering
\\vspace*{\\baselineskip}
{\\large{\\bf RECOVER}  \\\\[0.5\\baselineskip] 
  {\\bf OBSERVATIONAL STUDY MONITORING BOARD} \\\\[0.5\\baselineskip]
  {\\bf ", rep_dt_chr2, " REPORT}\\\\[2\\baselineskip] on the 
  {\\bf ", str_replace(rep_name, "OSMB", "Cohort"), "} } \\\\[6\\baselineskip] 
{\\large ", rep_dt_chr1, "}
\\vspace*{2\\baselineskip} \\\\

\\vspace{2.5in}

Prepared by: \\\\[\\baselineskip]
{\\large RECOVER Data Resource Core\\par}
\\vspace*{1\\baselineskip}
{ \\emph{MGH Biostatistics, Boston, MA} \\par}


\\vfill
\\includegraphics[scale=.8]{resources/MGH_logo.png} \\hspace{1.25in}
\\includegraphics[scale=.2]{resources/Harvard_logo.jpg}
\\end{titlepage}")
  
  tex_files_all <- list.files(path = paste0(outloc, "/tables/"),
                              pattern = "^s[0-9][0-9][0-9]o[0-9][0-9][0-9].+.tex$", 
                              full.names = TRUE, 
                              recursive = FALSE)
  ex_tabs <- list.files(path = paste0(outloc, "/tables/"),
                        pattern = "^s000o[0-9][0-9][0-9].+.tex$", 
                        full.names = TRUE, 
                        recursive = FALSE)
  
  # Remove execute summary tables
  
  tex_files <- setdiff(tex_files_all, ex_tabs)
  
  
  figure_files <- list.files(path = paste0(outloc, "/figures/"),
                             pattern = "^s[0-9][0-9][0-9]o[0-9][0-9][0-9].+.png$|*.jpg$",
                             full.names = TRUE,
                             recursive = F)
  
  all_files <- c(tex_files, figure_files)
  all_files <- all_files[order(gsub("figures|tables", "", all_files))]
  
  table_latex <- function(fl){
    
    print(fl)
    
    table_name <- fl 
    tmp_split <- unlist(strsplit(fl, "/"))
    file_str <- tmp_split[length(tmp_split)]
    
    footer_fl <- gsub("tables/", "footers/", gsub(".tex$", "_footer.tex", fl))
    footer_txt = latex_escape_fxn(paste(readLines(footer_fl), collapse="\n"))
    
    # tmp_remove_sec <- unlist(strsplit(file_str, "(?<=[a-zA-Z])\\s*(?=[0-9])", perl = T))
    # filename_clean <- latex_escape_fxn(tools::file_path_sans_ext(gsub("[[:digit:]]+", "", tmp_remove_sec)[3]))
    file_str_list <- str_split(file_str, "_")[[1]]
    file_ref <- file_str_list[1]
    filename_clean = gsub(".tex$", "",  file_str_list[2])
    
    paste0("
           
  \\footnotesize
  \\SetTblrInner{rowsep=4pt}
  \\import{tables/}{",
           gsub(".+/tables/", "", fl),
           "}
  \\normalsize
  \\clearpage"
    )
  }
  figure_latex <- function(fl){
    table_name <- fl 
    tmp_split <- unlist(strsplit(fl, "/"))
    file_str <- tmp_split[length(tmp_split)]
    # tmp_remove_sec <- unlist(strsplit(file_str, "(?<=[a-zA-Z])\\s*(?=[0-9])", perl = T))
    # filename_clean <- latex_escape_fxn(tools::file_path_sans_ext(gsub("[[:digit:]]+", "", tmp_remove_sec)[3]))
    file_str_list <- str_split(file_str, "_")[[1]]
    file_ref <- file_str_list[1]
    filename_clean = gsub("....$", "",  file_str_list[2])
    paste0("\\begin{figure}[h]
           \\centering
  \\caption{",
           filename_clean,
           "}
  \\label{fig:",
           file_ref,
           "}
  \\includegraphics[width=0.8\\textwidth,height=0.8\\textheight,keepaspectratio]{",
           gsub(".+/figures/", "figures/", fl),
           "}
  \\end{figure}
  \\clearpage"
    )
  }
  
  section = 0
  files_list <- list()
  for(output_fl in all_files){
    
    section_new = as.numeric(gsub(".+/s([0-9]+)o..._.+", "\\1", output_fl))
    object_out <- if(grepl("/tables/", output_fl)) {
      table_latex(output_fl)
    } else if(grepl("/figures/", output_fl)){
      figure_latex(output_fl)
    } 
    if(section_new == section) {
      files_list <- c(files_list, list(object_out))
    } else {
      if(!section_new %in% section_titles$section) {
        st_label = paste("Section", section_new)
      } else {
        st_label <- section_titles$section_title[section_titles$section == section_new]
      }
      
      st = paste0(ifelse(grepl("appendi", st_label, ignore.case = T), "\\counterwithin{table}{section}\\renewcommand{\\thetable}{A\\arabic{table}}", ""),
                  "\\centerline{\\bf ", st_label, "}\n", 
                  "\\setcounter{table}{0}",
                  sep="\n")
      section = section_new
      files_list <- c(files_list, 
                      list(paste0(st, 
                                  object_out))) 
    }
  }
  
  end <- paste0()
  
  toc_txt <- "

\\clearpage
\\tableofcontents
\\listoftables
\\listoffigures
\\clearpage
  
  "
  if(executive_summary != ""){
    executive_summary = glue("\\section*{Executive summary} \\addcontentsline{toc}{section}{Executive summary}
                             
                             **executive_summary**",
                             .open="**", .close="**")
  }
  latex_fl <- paste(title_page, 
                    toc_txt,
                    edition_notice, 
                    executive_summary, 
                    paste(unlist(files_list), collapse="\n\n\n\n"), 
                    "\\end{document}\n", sep="\n\n\n\n")
  
  writeLines(latex_fl, file.path(outloc, "main.tex"))
  
  rf_loc <- file.path(outloc, "resources")
  dir.create(rf_loc)
  file.copy(list.files(file.path(rep_rt_user, "latex_resources"), full.names = T),
            rf_loc)
  sty_files <- list.files(rf_loc, pattern=".sty")
  
  res_fl <- paste(unlist(lapply(gsub(".sty", "", sty_files), function(x) paste0("\\usepackage{", x, "}"))),
                  collapse="\n")
  writeLines(res_fl, file.path(rf_loc, "load_packages.tex"))
  
  writeLines(paste0("pdflatex main.tex\nrename main.pdf \"", rep_name, "_", format(Sys.time(), "%Y%m%d_%H%M"), ".pdf\"\nPAUSE"), file.path(outloc, "main.bat"))
  
  latex_runs <- paste(rep("\"/usr/local/texlive/2022/bin/x86_64-linux/pdflatex\" main.tex", max(n_runs, 2)), collapse="\n")
  
  writeLines(paste0(latex_runs, "\ncp main.pdf \"", rep_name, "_", format(Sys.time(), "%Y%m%d_%H%M"), ".pdf\"\nrm main.pdf"), file.path(outloc, "main.sh"))
  system2("chmod", args = c("+x", file.path(outloc, 'main.sh')), wait=T)
  
  save_wd <- getwd()
  setwd(outloc)
  system2("./main.sh", wait = T)
  setwd(save_wd)
  
  
  add_fls <- function(str){
    all_fls <- list.files(file.path(outloc, str))
    if(length(all_fls) == 0) return(NULL)
    paste0(str, "/", all_fls)
  }
  
  zip::zip(zipfile = glue("{rep_name}_latex_{rep_dt_chr1}.zip"), 
           files = c("main.tex", "main.bat",
                     add_fls("tables"),
                     add_fls("tables_pres"),
                     add_fls("figures"),
                     add_fls("resources")),
           root = outloc,
           include_directories = TRUE)
  return()
  # zip up with files
}

# OSMB_tex("Y:/code_space/ws/auto_main/reports/adult_OSMB/20220617_0900",
#          "REPORT 1", "2022-01-01")

o_cntr_fxn <- function(type, nm, list=F, ext) {
  t_fldr <- "tables"
  p_fldr <- "tables_pres"
  f_fldr <- "figures"
  if(!"footers" %in% list.dirs(output_loc, full.names = F)) dir.create(file.path(output_loc, "footers"))
  
  if(!exists("o_cntr")) o_cntr <<- 1
  if(!exists("p_cntr")) p_cntr <<- 1
  if(type == "t") {
    if(!t_fldr %in% list.dirs(output_loc, full.names = F)) dir.create(file.path(output_loc, t_fldr))
  } else if(type == "f") {
    if(!f_fldr %in% list.dirs(output_loc, full.names = F)) dir.create(file.path(output_loc, f_fldr))
  } else {
    if(!p_fldr %in% list.dirs(output_loc, full.names = F)) dir.create(file.path(output_loc, p_fldr))
  }
  str_cntr <- ifelse(type == "p", p_cntr, o_cntr)
  str_code <- sprintf("s%03do%03d", s, str_cntr)
  str_type = case_when(type == 't' ~ t_fldr,
                       type == 'p' ~ p_fldr,
                       T ~ f_fldr)
  if(missing(ext)){
    str_ext <- ifelse(type %in% c('p', 't'), 'tex', 'png')
  } else {
    str_ext <- ext
  }
  str_nm <- glue("{str_code}_{nm}.{str_ext}")
  str_out <- glue("{output_loc}/{str_type}/{str_nm}")
  
  if(type == "p") {
    p_cntr <<- p_cntr + 1
  } else {
    o_cntr <<- o_cntr + 1
  }
  
  
  cc_heading <- fp_text(color = "#4472c0",
                        bold=T, 
                        # font.family = "MV Boli",
                        font.size = 16)
  
  format_cc_heading <- ftext(nm, cc_heading) %>% fpar()
  
  if(list) {
    list(fl = str_out,
         file_prefix = str_code,
         file_nm = str_nm,
         file_n = str_cntr,
         docx_head= format_cc_heading) 
  } else {
    str_out
  }
}

mk_latex_tb_ref <- function(o_cntr_obj) {
  glue("{\\bf Table~\\ref{tab:**o_cntr_obj$file_prefix**}}", .open = "**", .close = "**")
}

mk_latex_fig_ref <- function(o_cntr_obj) {
  glue("{\\bf Figure~\\ref{fig:**o_cntr_obj$file_prefix**}}", .open = "**", .close = "**")
}

mk_latex_tb_ins <- function(o_cntr_obj, pb = F) {
  
  glue("
  **ifelse(pb, '\\\\pagebreak', '')**
  \\footnotesize
  \\SetTblrInner{rowsep=4pt}
  \\import{tables/}{**o_cntr_obj$file_nm**}
  
  \\normalsize
  \\bigskip
  
  ",
       .open = "**", .close = "**")
}

## a similar function using a pivot

## Note that categories cannot overlap. So if "unknown" is nested below "unknown" then it will break. try using explicit naming: "Sex Unknown"
summ_tab_span = function(a, v_tar, v_by_group = NA, spanh1_only = T, missing_as = "Missing") {
  
  if(spanh1_only) {
    ## this spanning structure looks weird
    nm_sf <- paste(v_by_group[1], paste(rep("%s", length(v_by_group)), collapse="."), sep=".")
  } else {
    ## this spanning structure matches the request but sub spans needs self explanatory levels
    nm_sf <- paste0(v_by_group, ".%s", collapse=".")
  }
  
  v_splits = length(levels(a[[tail(v_by_group, 1)]]))
  
  add_trow <- function(ds){
    trow <- ds %>% 
      ungroup() %>% 
      summarise(across(all_of(v_tar), ~ "Overall"),
                across(-all_of(v_tar), sum))
    bind_rows(ds, trow)
  }
  
  tbl_table_process = a %>% 
    select(all_of(c(v_tar, v_by_group))) %>% 
    mutate(
      temp1 := case_when(
        !!sym(v_tar[ifelse(length(v_tar)>=2,2,1)]) %in% c(NA, "") ~ missing_as, 
        T ~ as.character(!!sym(v_tar[ifelse(length(v_tar)>=2,2,1)]))
      ), 
      temp1_f = factor(
        temp1, 
        levels = unique(c(levels(!!sym(v_tar[ifelse(length(v_tar)>=2,2,1)])), missing_as))
      ), 
      !!sym(v_tar[ifelse(length(v_tar)>=2,2,1)]) := temp1_f
    ) %>%
    group_by(!!!syms(c(v_tar, v_by_group)), .drop = F) %>% 
    summarise(n=n(),
              .groups="drop") %>%
    mutate(colnm = do.call("sprintf", list(nm_sf, !!!syms(v_by_group)))) %>% 
    select(-all_of(v_by_group)) %>% 
    pivot_wider(names_from = colnm,
                values_from = n,
                names_sep=".") %>% 
    mutate(across(-all_of(v_tar), replace_na, 0)) %>% 
    add_trow() %>% 
    mutate(Overall = rowSums(across(-all_of(v_tar)))) 
  
  v_hline = grep(levels(a[[tail(v_tar,1)]])[1], tbl_table_process[[v_tar[length(v_tar)]]])
  
  if (length(v_hline) > 1) {
    v_hline = v_hline - 1
    v_hline = v_hline[-1]
    v_hline = c(v_hline, (nrow(tbl_table_process) - 1))
  } else {
    v_hline = nrow(tbl_table_process) - 1
  }
  
  flx_output = tbl_table_process %>%
    as_flextable() %>% 
    span_header() %>%
    vline(j = seq(from = ifelse(length(v_tar) == 1, 1, 2), to = length(.$body$col_keys), 
                  by = v_splits), part = "all") %>%
    merge_v(j = 1) %>%
    merge_h_range(i = nrow(.$body$dataset), j1 = 1, j2 = ifelse(length(v_tar) == 1, 1, 2), 
                  part = "body") %>%
    hline(i = v_hline) %>%
    border_outer() %>% 
    align(align="center", part="all")
  return(flx_output)
}

### takes a summ_tab object and converts the percentages to row percentages -- useful for certain outputs
# input summ_tab object (st) and names of cols to convert
## WARNING: right now this only works if all variables are categorical (no Mean values)

summ_tab_row <- function(st, cols) {
  st %>%
    mutate(across(all_of(c(cols, "Overall")), ~ as.numeric(gsub( " .*$", "", .x)))) %>%
    mutate(across(all_of(cols), ~ paste0(.x, " (", round(.x/Overall*100, 0), "%)")))
}







end_fxns <- as.vector(lsf.str())
message("flre_list added for opening files.")
# can this print out the cohorts that are actually available?
message("'cong', 'ped', 'adult', and 'autopsy' cohorts available from get_env_list.")
message(paste("New Functions Added:", paste(setdiff(end_fxns, curr_fxns), collapse=", ")))

# I don't know why this is needed but if the system crashes it seems to reboot with options(warn=2)
options(warn=1)

# this helps with dealing with some shell runs of scripts. This will break if we ever switch away from the current rstudio server
Sys.setenv(RSTUDIO_PANDOC="/usr/lib/rstudio-server/bin/quarto/bin/tools/x86_64")


