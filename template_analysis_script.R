top_dir <- system("git rev-parse --show-toplevel", intern = T, ignore.stderr = T)
if(length(top_dir) == 0) top_dir = "."
setwd(top_dir)


source("https://raw.githubusercontent.com/jchan12-mgh/general_use_scripts/refs/heads/main/helper_fxns.R")

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

cong_env_list <- get_env_list("/home/shared/dcc_test/peds_comb/cong")

ds_dd <- cong_env_list$ds_dd()
formds_list <- cong_env_list$formds_list()



qs_docx <- read_docx() %>%  # read_docx(path = glue("{rep_rt_user}/../../../DM/supplementary_datasets/template_tabs.docx")) %>% 
  body_add("Example Report", style="centered") %>% 
  body_add_toc(level = 2) %>% 
  body_add_break() %>% 
  docx_head1("Notes on this report") %>% 
  body_add_normal("Note 1") %>% 
  body_add_normal("Note 2") %>% 
  body_add_break() %>% 
  docx_head1("Section 1") %>% 
  docx_head2("Table 1.1") %>% 
  body_add_flextable_font(data.frame())  %>% 
  body_add_break() %>% 
  docx_head2("Table 1.2") %>% 
  body_add_flextable_font(data.frame()) %>% 
  body_add_break() %>% 
  docx_head1("Section 2") %>% 
  docx_head2("Table 2.1") %>% 
  body_add_flextable_font(data.frame())

print(qs_docx, glue("{output_loc}/example_report_{today_tm}.docx"))


print(glue("Complete and saved to {output_loc}"))

### closing log file -----

closeAllConnections()

print(glue("Complete and saved to {output_loc}"))