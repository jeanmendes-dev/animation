# animation

read_any_dataset <- function(path) {

  ext <- tolower(tools::file_ext(path))

  if (ext == "sas7bdat") {

    df <- haven::read_sas(path)

  } else if (ext == "csv") {

    # USAR fread (mais robusto para Domino + arquivos grandes)
    df <- data.table::fread(path, sep = "auto", data.table = FALSE)

  } else {
    stop("Unsupported file type: ", path)
  }

  # Padronização CDISC
  df %>%
    rename_with(~ toupper(.x))
}
