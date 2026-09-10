from __future__ import annotations
from pathlib import Path
from pydantic import Field

from classes.data.files.file_basic import FileBasic

class BamONT(FileBasic):
    """
    Метаданные файла BAM, полученного из данных ONT
    """
    batch: str = Field(default='UNDEFINED', description='Идентификатор батча')
    pore: str = Field(default='UNDEFINED', description='Тип поры ONT', examples=['r941', 'r1041', 'rp4'])
    model: str = Field(default='UNDEFINED', description='Модель бейсколлинга')
    molecule: str = Field(default='UNDEFINED', description='Тип исходной молекулы', examples=['dna', 'rna'])
    modifications: list[str] = Field(default=[], description='Список модификаций, указанных при бейсколлинге')
    per_read_alignment_stats: Path|None = Field(
                                                 default=None,
                                                 description="Bamstats per-read output TSV file (compressed with gzip)."
                                                )
    per_reference_alignment_stats: Path|None = Field(
                                                     default=None,
                                                     description="Bamstats flagstat output TSV file."
                                                    )
    alignment_accuracy_histogram: Path|None = Field(
                                                    default=None,
                                                    description="Bamstats alignment accuracy histogram TSV file."
                                                   )
    alignment_coverage_histogram: Path|None = Field(
                                                    default=None,
                                                    description="Bamstats alignment coverage histogram TSV file."
                                                   )
    read_length_histogram_mapped: Path|None = Field(
                                                    default=None,
                                                    description="Bamstats read length histogram TSV file (for mapped reads)."
                                                   )
    read_length_histogram_unmapped: Path|None = Field(
                                                      default=None,
                                                      description="Bamstats read length histogram TSV file (for unmapped reads)."
                                                     )
    read_quality_histogram_mapped: Path|None = Field(
                                                    default=None,
                                                    description="Bamstats read quality histogram TSV file (for mapped reads)."
                                                   )
    read_quality_histogram_unmapped: Path|None = Field(
                                                       default=None,
                                                       description="Bamstats read quality histogram TSV file (for unmapped reads)."
                                                      )
    alignments_index_file: Path|None = Field(
                                             default=None,
                                             description="Index for alignments BAM file."
                                            )
    igv_config_json_file: Path|None = Field(
                                            default=None,
                                            description="JSON file with IGV config options to be used by the EPI2ME Desktop Application."
                                           )
