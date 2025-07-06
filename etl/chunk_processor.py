# first party imports
import os
from datetime import datetime, timezone
from functools import reduce
import logging
import operator
import pandas as pd
import uuid


# third party imports
import config

logger = logging.getLogger(__name__)

def process_chunk(chunk_number: int ,chunk: pd.DataFrame) -> str:
    logger.info(f"processing chunk with chunk_number: {chunk_number}")
    try:
        df = chunk.copy()
        transformation_config = config.get_transformation_config()
        coerce_steps = [s for s in transformation_config if s['action'] == 'coerce_to_numeric']
        standardize_steps = [s for s in transformation_config if s['action'] == 'standardize_column']
        add_column_steps = [s for s in transformation_config if s['action'] == 'add_column']
        filter_steps = [s for s in transformation_config if s['action'] == 'filter_rows']

        # step 1 
        for step in coerce_steps:
            field = step['field_name']
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
                if df[field].isna().any():
                    df.dropna(subset=[field], inplace=True)

        # step 2 
        assignments = {}
        for step in standardize_steps:
            field = step['field_name']
            style = step['type']
            if field in df.columns:
                if style == 'lower':
                    assignments[field] = df[field].str.lower()
                elif style == 'upper':
                    assignments[field] = df[field].str.upper()
            
        for step in add_column_steps:
            field = step['field_name']
            value_type = step['value_type']
            if value_type == 'utcnow':
                assignments[field] = datetime.now(timezone.utc).isoformat()

        if assignments:
            df = df.assign(**assignments)

        # step 3 
        filter_masks = []
        op_map = {
            "equal": operator.eq, "not_equal": operator.ne,
            "greater_than": operator.gt, "greater_than_or_equal_to": operator.ge,
            "less_than": operator.lt, "less_than_or_equal_to": operator.le,
        }
        
        for step in filter_steps:
            field = step['field_name']
            condition = step['condition']
            value = step['value']
            if field in df.columns and condition in op_map:
                mask = op_map[condition](df[field], value)
                filter_masks.append(mask)
        
        if filter_masks:
            final_mask = reduce(operator.and_, filter_masks)
            df = df[final_mask]

        temp_file_path = os.path.join(config.TEMP_FILES_DIR, f"processed_chunk_{uuid.uuid4()}.csv")
        df.to_csv(temp_file_path, index=False)
        return temp_file_path
    
    except Exception as ex:
        logger.error(f"An unexpected error occurred during chunk transformation: {ex}", exc_info=True)
        return None
