FROM maven:3.9.9-amazoncorretto-24 AS build

RUN yum update -y && yum install -y git && yum clean all

WORKDIR /app

# Seed container-local Maven repo from build context (staged by Jenkins script)
COPY .m2 /root/.m2

# Repo root must be the build context
COPY . .

# Build only ETL + its reactor deps; force offline so it never tries GitHub Packages
RUN mvn -o -nsu -f /app/pom.xml -B -pl etl -am clean package -DskipTests

# Sanity check expected jars
RUN ls -la /app/docker/pic-sure-hpds-etl && \
    find /app/docker/pic-sure-hpds-etl -maxdepth 1 -type f -name "*-jar-with-dependencies.jar" -print


FROM amazoncorretto:24-alpine

RUN apk add --no-cache --purge -uU bash curl wget unzip gnupg openssl && \
    rm -rf /var/cache/apk/* /tmp/*

WORKDIR /app
COPY --from=build /app/docker/pic-sure-hpds-etl/SQLLoader-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/CSVLoader-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/CSVLoaderNewSearch-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/CSVDumper-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/VCFLocalLoader-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/SplitChromosomeVcfLoader-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/VariantMetadataLoader-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/GenomicDatasetFinalizer-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/UnifiedVCFLocalLoader-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/MultialleleCounter-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/RekeyDataset-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/RemoveConceptFromMetadata-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/HideAnnotationCategoryValue-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/SequentialLoader-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/LowRAMMultiCSVLoader-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/CreateColumnmetaCSV-jar-with-dependencies.jar .
COPY --from=build /app/docker/pic-sure-hpds-etl/create_key.sh .

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Xmx${HEAPSIZE:-2048}m -jar ${LOADER_NAME:-CSVLoader}-jar-with-dependencies.jar $LOADER_ARGS"]