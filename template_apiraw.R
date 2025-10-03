top_dir <- system("git rev-parse --show-toplevel", intern = T, ignore.stderr = T)
if(length(top_dir) == 0) top_dir = "."
setwd(top_dir)


source("https://raw.githubusercontent.com/jchan12-mgh/general_use_scripts/refs/heads/main/helper_fxns.R")
# How is a secure private location managed for tokens?
source("~/load_all_tokens.R")

# Required directory structure
# rt is the project folder
# rt has DM_src, DM, and codespace folders

rt <- "path_to_root"
# Strings in here will become the folder name for that project
all_projects <- c("project_name")


# api url should end in api/

urlapi <- "https://redcap.partners.org/redcap/api/" # "https://recover-redcap.partners.org/api/"

today_tm <- paste(format(Sys.Date(), "%Y-%m-%d"), format(Sys.time(),"%H_%M"), sep="_")
today <- format(Sys.Date(), "%Y-%m-%d")

main_api_out<- file.path(top_dir, "../reports/main_api_run")
dir.create(main_api_out, recursive = T)


start_sink <- function(append=T) {
  sink(glue("{main_api_out}/main_api_run_{today_tm}.log"), split=T, append=append)
  sink(type = "message", append=append)
}

start_sink(append=F)

loc_list <- list()

for(proj in all_projects){ 
  loc_list[[proj]] <- get_loc(proj)
}

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 20)

# specified enrollment form is necessary because an initial list of all IDs to load is needed
# see if there is a better way to do this. Just load all record_ids? Will sill need to make sure record_id is unique identifier
# think of change for shape of data when loaded
# api call should read in a single form and write out that single form


cat(glue("------------------- starting rc_project - {format(Sys.time(), '%H:%M')} ---------------- \n\n"))
data_rc_project <- get_rc_formdata(token_rc_project, "rc_project", url = urlapi)

cat(glue("------------------- Complete - {format(Sys.time(), '%H:%M')} ---------------------- \n\n"))
closeAllConnections()


