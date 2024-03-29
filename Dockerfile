FROM python:3.10
WORKDIR /data
ENV STAGEDIR /data
COPY requirements.txt ${STAGEDIR}
RUN python3 -m pip install -r /data/requirements.txt
#COPY site-packages.zip ${STAGEDIR}/
#RUN cd /data && \
#	unzip site-packages.zip  && \
#	cp -r site-packages/*  /usr/local/lib/python3.7/site-packages/ && \
#	rm site-packages.zip && \ 
#	rm -rf site-packages
COPY download_and_upload_to_ICAv2.py ${STAGEDIR}/
COPY etagTomd5sum.py ${STAGEDIR}/
COPY 272296024.fastq.signedurl.json ${STAGEDIR}/
COPY test.metadata_table.csv ${STAGEDIR}/
COPY etagTomd5sum.py /usr/local/lib/python3.10/site-packages/
COPY download_and_upload_run_to_ICAv2.py ${STAGEDIR}/
COPY download_and_upload_to_ICAv2.rename_fastqs.py ${STAGEDIR}/
COPY rename_FASTQ.py /usr/local/lib/python3.10/site-packages/
COPY retry_requests_decorator.py /usr/local/lib/python3.10/site-packages/
ENV PATH $PATH:${STAGEDIR}