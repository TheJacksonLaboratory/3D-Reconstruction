FROM nfcore/base:1.9

LABEL authors="cgpu" \
      description="Docker image containing dependencies for .."

COPY environment.yml /
RUN conda env create -f /environment.yml && conda clean -a
ENV PATH /opt/conda/envs/a_repo/bin:$PATH