configfile: "config.yml"

rule fetch_fastq:
    group:
        "kallisto_sample"
    params:
        url=lambda wildcards: config['FASTQ_URLS'][wildcards.ID]
    log:
        "logs/data/fastq/{ID}.log"
    output:
        temp("data/fastq/{ID}.fastq.gz")
    shell:
        "wget -O {output} --no-verbose {params.url} 2>&1 | tee {log}"

rule run_kallisto:
    group:
        "kallisto_sample"
    input:
        "data/fastq/{ID}.fastq.gz"
    log:
        "logs/data/kallisto/{ID}.log"
    output:
        "data/kallisto/{ID}/abundance.tsv"
    shell:
        "kallisto quant --index={config[INDEX_FILE]} --output-dir=$(dirname {output}) --single -l 50 -s 2 {input} 2>&1 | tee {log}"

rule generate_count_matrix:
    input:
        expand("data/kallisto/{ID}/abundance.tsv",
               ID=list(config['FASTQ_URLS'].keys()))
    output:
        df="data/tpm.tsv"
    run:
        import os
        import pandas as pd
        tpms = []
        names = []
        print(input)
        print(output.df)

        transcript_info = pd.read_csv("mus_musculus/transcripts_to_genes.txt", sep="\t", header=None, index_col=0)
        transcript_info.columns = ['GENE_ID', 'GENE_SYMBOL']

        for file in input:
            print(file)
            df = pd.read_csv(file, sep="\t")
            df['gene'] = df['target_id'].map(transcript_info['GENE_ID'])
            tpm = df.groupby('gene').sum()['tpm']
            tpms.append(tpm)
            names.append(os.path.basename(os.path.dirname(file)))
            print(len(names))

        combined = pd.DataFrame(tpms, index=names)
        combined.to_csv(output.df, sep="\t", header=True, index=True)

