from shapely.geometry.base import BaseGeometry
from blobhand.blob.reader import BlobServiceClientReader


def test_csv_to_dataframe():
    # Arrange
    conn_str = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
    container = "test-container"
    blob_name = "test.csv"

    # Act
    reader = BlobServiceClientReader.from_connection_string(conn_str)
    df = reader.csv_to_dataframe(container=container, blob_name=blob_name)

    # Assert
    assert df.shape == (10000, 5)

def test_parquet_to_dataframe():
    # Arrange
    conn_str = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
    container = "test-container"
    blob_name = "test.parquet"

    # Act
    reader = BlobServiceClientReader.from_connection_string(conn_str)
    df = reader.parquet_to_dataframe(container=container, blob_name=blob_name)

    # Assert
    assert df.shape == (875, 4)

def test_geoparquet_to_geodataframe():
    # Arrange
    conn_str = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
    container = "test-container"
    blob_name = "test.geoparquet"

    # Act
    reader = BlobServiceClientReader.from_connection_string(conn_str)
    df = reader.geoparquet_to_geodataframe(container=container, blob_name=blob_name)

    # Assert
    df_dtypes = df.dtypes.to_list()
    types = [dtype.type for dtype in df_dtypes]

    if BaseGeometry in types:
        assert True

    assert df.shape == (2817, 10)

def test_geojson_to_geodataframe():
    # Arrange
    conn_str = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
    container = "test-container"
    blob_name = "test.geojson"

    # Act
    reader = BlobServiceClientReader.from_connection_string(conn_str)
    df = reader.geojson_to_geodaframe(container=container, blob_name=blob_name)

    # Assert
    df_dtypes = df.dtypes.to_list()
    types = [dtype.type for dtype in df_dtypes]

    if BaseGeometry in types:
        assert True

    assert df.shape == (20, 23)