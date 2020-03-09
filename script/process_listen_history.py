import sys
sys.path.append(".")

from MusicalGraph.ListenHistoryPipeline import ListenHistoryPipeline


if __name__ == "__main__":
    pipeline = ListenHistoryPipeline()
    pipeline.process_s1()
    pipeline.process_s2()
    pipeline.process_s3()
