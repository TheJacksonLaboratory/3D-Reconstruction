/*
 * Copyright (c) 202x, {{organisation name}} and the authors.
 *
 *   This file is part of {{name-of-repo}} a pipeline repository to ... .
 *
 * Main {{name-of-repo}} pipeline script
 *
 * @authors
 * Name1 LastName1 <name.lastname1@email.com>
 * Name2 LastName2 <name.lastname2@email.com>
 */

log.info "{{Do this awesome task with repo-name }} version 0.1"
log.info "====================================="
log.info "Results directory : ${params.output}"
log.info "\n"

def helpMessage() {
    log.info """
    Usage:
    The typical command for running the pipeline is as follows:
    nextflow run {{github-username}}/{{repo-name}} --output 'results' -profile docker
    Mandatory arguments:
      --input                   Path to input file. The suffix of the file must be .csv
                                The csv file is expected to have two columns  (no header.
                                Column 1, must correspond a unique name characterising the tar.gz file
                                Column 2, must correspond to a filepath of a tar.gz file

      -profile                  Configuration profile to use. Can use multiple (comma separated)
                                Available: testdata, docker, ...
    Optional:
      --ouput                   Path to output directory. 
                                Default: 'results'

      --threshold               Threshold for a p-value or filtering cutoff. 
                                Default: 0.25    
    """.stripIndent()
}

/*********************************
 *      CHANNELS SETUP           *
 *********************************/

// Input

// Input list .csv file of many .tar.gz
if (params.input.endsWith(".csv")) {
  Channel.fromPath(params.input)
                        .ifEmpty { exit 1, "Input .csv list of .tar.gz files not found at ${params.input}. Is the file path correct?" }
                        .splitCsv(sep: ',')
                        .map { unique_id, path -> tuple(unique_id+".tar.gz", file(path)) }
                        .set { archiveInputFiles }
  }


/*********************************
 *          PROCESSES            *
 *********************************/

/*
 * Do something usseful as a first step
 */

 process firstProcessName {
    tag "${name}"
    label 'process_config_label'

    input:
    set val(name), file(archive) from archiveInputFiles

    output:
    file "${name}" into processedFiles

    script:
    """
    mv ${archive} ${name}
    """
}

/*
 * Do something with the output data of the 'firstProcessName' process
 *          &
 * then do some more!
 */

 process secondProcessName {
    tag "doing sth with ${oneProcessedFile}"
    label 'another_process_config_label'

    input:
    file(oneProcessedFile) from processedFiles

    output:
    file "${newly_named_processed_File}" into renamedFiles

    script:
    """
    cp $oneProcessedFile newly_named_processed_File
    """
}