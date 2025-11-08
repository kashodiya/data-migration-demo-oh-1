


"""
Data Transformation Engine

Transforms SQLite relational data into optimized DynamoDB NoSQL format
based on access patterns and denormalization strategies.
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import json
from sqlite_analyzer import SQLiteAnalyzer


class DataTransformer:
    """Transforms SQLite data to DynamoDB format"""
    
    def __init__(self, config: Dict[str, Any], logger, sqlite_analyzer: SQLiteAnalyzer):
        """
        Initialize data transformer
        
        Args:
            config: Migration configuration
            logger: Logger instance
            sqlite_analyzer: SQLite database analyzer
        """
        self.config = config
        self.logger = logger
        self.analyzer = sqlite_analyzer
        self.table_prefix = config['table_prefix']
        
        # Cache for denormalized data lookups
        self._lookup_cache = {}
    
    def transform_music_catalog_data(self, source_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Transform music catalog data (Artists, Albums, Tracks) into denormalized DynamoDB items
        
        Args:
            source_data: Dictionary containing artists, albums, tracks, genres, and media types
            
        Returns:
            List of DynamoDB items for MusicCatalog table
        """
        items = []
        
        # Build lookup caches for denormalization
        self._build_lookup_caches(source_data)
        
        # Transform Artists
        for artist in source_data.get('Artist', []):
            items.append(self._transform_artist(artist))
        
        # Transform Albums with artist information
        for album in source_data.get('Album', []):
            items.append(self._transform_album(album))
        
        # Transform Tracks with full denormalized data
        for track in source_data.get('Track', []):
            items.append(self._transform_track(track))
        
        self.logger.info(f"Transformed {len(items)} music catalog items")
        return items
    
    def transform_customer_data(self, source_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Transform customer and invoice data into DynamoDB items
        
        Args:
            source_data: Dictionary containing customers, invoices, and invoice lines
            
        Returns:
            List of DynamoDB items for CustomerData table
        """
        items = []
        
        # Build invoice lines lookup
        invoice_lines_by_invoice = {}
        for line in source_data.get('InvoiceLine', []):
            invoice_id = line['InvoiceId']
            if invoice_id not in invoice_lines_by_invoice:
                invoice_lines_by_invoice[invoice_id] = []
            invoice_lines_by_invoice[invoice_id].append(line)
        
        # Transform Customers
        for customer in source_data.get('Customer', []):
            items.append(self._transform_customer(customer))
        
        # Transform Invoices with embedded line items
        for invoice in source_data.get('Invoice', []):
            invoice_lines = invoice_lines_by_invoice.get(invoice['InvoiceId'], [])
            items.append(self._transform_invoice(invoice, invoice_lines))
        
        self.logger.info(f"Transformed {len(items)} customer data items")
        return items
    
    def transform_playlist_data(self, source_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Transform playlist data into DynamoDB items
        
        Args:
            source_data: Dictionary containing playlists and playlist tracks
            
        Returns:
            List of DynamoDB items for PlaylistData table
        """
        items = []
        
        # Build track lookup for denormalization
        track_lookup = {track['TrackId']: track for track in source_data.get('Track', [])}
        
        # Transform Playlists
        for playlist in source_data.get('Playlist', []):
            items.append(self._transform_playlist(playlist))
        
        # Transform Playlist Tracks with denormalized track info
        for playlist_track in source_data.get('PlaylistTrack', []):
            track_info = track_lookup.get(playlist_track['TrackId'], {})
            items.append(self._transform_playlist_track(playlist_track, track_info))
        
        self.logger.info(f"Transformed {len(items)} playlist data items")
        return items
    
    def transform_employee_data(self, source_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Transform employee data into DynamoDB items
        
        Args:
            source_data: Dictionary containing employees
            
        Returns:
            List of DynamoDB items for EmployeeData table
        """
        items = []
        
        # Transform Employees
        for employee in source_data.get('Employee', []):
            items.append(self._transform_employee(employee))
        
        self.logger.info(f"Transformed {len(items)} employee data items")
        return items
    
    def _build_lookup_caches(self, source_data: Dict[str, List[Dict[str, Any]]]):
        """Build lookup caches for denormalization"""
        
        # Artist lookup
        self._lookup_cache['artists'] = {
            artist['ArtistId']: artist for artist in source_data.get('Artist', [])
        }
        
        # Album lookup
        self._lookup_cache['albums'] = {
            album['AlbumId']: album for album in source_data.get('Album', [])
        }
        
        # Genre lookup
        self._lookup_cache['genres'] = {
            genre['GenreId']: genre for genre in source_data.get('Genre', [])
        }
        
        # MediaType lookup
        self._lookup_cache['media_types'] = {
            media['MediaTypeId']: media for media in source_data.get('MediaType', [])
        }
        
        # Employee lookup
        self._lookup_cache['employees'] = {
            emp['EmployeeId']: emp for emp in source_data.get('Employee', [])
        }
    
    def _transform_artist(self, artist: Dict[str, Any]) -> Dict[str, Any]:
        """Transform artist record to DynamoDB item"""
        artist_id = artist['ArtistId']
        
        return {
            'PK': f"ARTIST#{artist_id}",
            'SK': 'METADATA',
            'EntityType': 'Artist',
            'ArtistId': artist_id,
            'Name': artist.get('Name', ''),
            'GSI1PK': 'ARTIST',
            'GSI1SK': artist.get('Name', '').upper(),
            'GSI2PK': f"SEARCH#{artist.get('Name', '').upper()}",
            'GSI2SK': f"ARTIST#{artist_id}",
            'CreatedAt': datetime.utcnow().isoformat(),
            'UpdatedAt': datetime.utcnow().isoformat()
        }
    
    def _transform_album(self, album: Dict[str, Any]) -> Dict[str, Any]:
        """Transform album record to DynamoDB item"""
        album_id = album['AlbumId']
        artist_id = album['ArtistId']
        
        # Get artist information for denormalization
        artist = self._lookup_cache['artists'].get(artist_id, {})
        artist_name = artist.get('Name', 'Unknown Artist')
        
        return {
            'PK': f"ALBUM#{album_id}",
            'SK': 'METADATA',
            'EntityType': 'Album',
            'AlbumId': album_id,
            'Title': album.get('Title', ''),
            'ArtistId': artist_id,
            'ArtistName': artist_name,
            'GSI1PK': f"ARTIST#{artist_id}",
            'GSI1SK': f"ALBUM#{album.get('Title', '').upper()}",
            'GSI2PK': f"SEARCH#{album.get('Title', '').upper()}",
            'GSI2SK': f"ALBUM#{album_id}",
            'CreatedAt': datetime.utcnow().isoformat(),
            'UpdatedAt': datetime.utcnow().isoformat()
        }
    
    def _transform_track(self, track: Dict[str, Any]) -> Dict[str, Any]:
        """Transform track record to DynamoDB item with full denormalization"""
        track_id = track['TrackId']
        album_id = track.get('AlbumId')
        genre_id = track.get('GenreId')
        media_type_id = track['MediaTypeId']
        
        # Get related information for denormalization
        album = self._lookup_cache['albums'].get(album_id, {}) if album_id else {}
        artist = self._lookup_cache['artists'].get(album.get('ArtistId'), {}) if album else {}
        genre = self._lookup_cache['genres'].get(genre_id, {}) if genre_id else {}
        media_type = self._lookup_cache['media_types'].get(media_type_id, {})
        
        item = {
            'PK': f"TRACK#{track_id}",
            'SK': 'METADATA',
            'EntityType': 'Track',
            'TrackId': track_id,
            'Name': track.get('Name', ''),
            'Composer': track.get('Composer', ''),
            'Milliseconds': track.get('Milliseconds', 0),
            'Bytes': track.get('Bytes', 0),
            'UnitPrice': float(track.get('UnitPrice', 0.0)),
            'MediaTypeId': media_type_id,
            'MediaTypeName': media_type.get('Name', 'Unknown'),
            'CreatedAt': datetime.utcnow().isoformat(),
            'UpdatedAt': datetime.utcnow().isoformat()
        }
        
        # Add album information if available
        if album:
            item.update({
                'AlbumId': album_id,
                'AlbumTitle': album.get('Title', ''),
                'ArtistId': album.get('ArtistId'),
                'ArtistName': artist.get('Name', 'Unknown Artist')
            })
            
            # GSI1 for album-based access
            item['GSI1PK'] = f"ALBUM#{album_id}"
            item['GSI1SK'] = f"TRACK#{track.get('Name', '').upper()}"
        else:
            item['ArtistName'] = 'Unknown Artist'
            item['GSI1PK'] = 'NO_ALBUM'
            item['GSI1SK'] = f"TRACK#{track.get('Name', '').upper()}"
        
        # Add genre information if available
        if genre:
            item.update({
                'GenreId': genre_id,
                'GenreName': genre.get('Name', 'Unknown')
            })
        
        # GSI2 for search functionality
        item['GSI2PK'] = f"SEARCH#{track.get('Name', '').upper()}"
        item['GSI2SK'] = f"TRACK#{track_id}"
        
        return item
    
    def _transform_customer(self, customer: Dict[str, Any]) -> Dict[str, Any]:
        """Transform customer record to DynamoDB item"""
        customer_id = customer['CustomerId']
        support_rep_id = customer.get('SupportRepId')
        
        # Get support rep information
        support_rep = self._lookup_cache.get('employees', {}).get(support_rep_id, {}) if support_rep_id else {}
        
        item = {
            'PK': f"CUSTOMER#{customer_id}",
            'SK': 'PROFILE',
            'EntityType': 'CustomerProfile',
            'CustomerId': customer_id,
            'FirstName': customer.get('FirstName', ''),
            'LastName': customer.get('LastName', ''),
            'Company': customer.get('Company', ''),
            'Address': customer.get('Address', ''),
            'City': customer.get('City', ''),
            'State': customer.get('State', ''),
            'Country': customer.get('Country', ''),
            'PostalCode': customer.get('PostalCode', ''),
            'Phone': customer.get('Phone', ''),
            'Fax': customer.get('Fax', ''),
            'Email': customer.get('Email', ''),
            'GSI1PK': customer.get('Email', '').lower(),  # For email-based login
            'GSI1SK': f"CUSTOMER#{customer_id}",
            'CreatedAt': datetime.utcnow().isoformat(),
            'UpdatedAt': datetime.utcnow().isoformat()
        }
        
        # Add support rep information
        if support_rep_id:
            item.update({
                'SupportRepId': support_rep_id,
                'SupportRepName': f"{support_rep.get('FirstName', '')} {support_rep.get('LastName', '')}".strip(),
                'GSI2PK': f"SUPPORT_REP#{support_rep_id}",
                'GSI2SK': f"CUSTOMER#{customer_id}"
            })
        
        return item
    
    def _transform_invoice(self, invoice: Dict[str, Any], invoice_lines: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Transform invoice with embedded line items"""
        invoice_id = invoice['InvoiceId']
        customer_id = invoice['CustomerId']
        
        # Transform invoice lines
        transformed_lines = []
        for line in invoice_lines:
            transformed_lines.append({
                'InvoiceLineId': line['InvoiceLineId'],
                'TrackId': line['TrackId'],
                'UnitPrice': float(line['UnitPrice']),
                'Quantity': line['Quantity'],
                'LineTotal': float(line['UnitPrice']) * line['Quantity']
            })
        
        return {
            'PK': f"CUSTOMER#{customer_id}",
            'SK': f"INVOICE#{invoice_id}",
            'EntityType': 'Invoice',
            'InvoiceId': invoice_id,
            'CustomerId': customer_id,
            'InvoiceDate': invoice.get('InvoiceDate', ''),
            'BillingAddress': invoice.get('BillingAddress', ''),
            'BillingCity': invoice.get('BillingCity', ''),
            'BillingState': invoice.get('BillingState', ''),
            'BillingCountry': invoice.get('BillingCountry', ''),
            'BillingPostalCode': invoice.get('BillingPostalCode', ''),
            'Total': float(invoice.get('Total', 0.0)),
            'InvoiceLines': transformed_lines,
            'LineCount': len(transformed_lines),
            'CreatedAt': datetime.utcnow().isoformat(),
            'UpdatedAt': datetime.utcnow().isoformat()
        }
    
    def _transform_playlist(self, playlist: Dict[str, Any]) -> Dict[str, Any]:
        """Transform playlist record to DynamoDB item"""
        playlist_id = playlist['PlaylistId']
        
        return {
            'PK': f"PLAYLIST#{playlist_id}",
            'SK': 'METADATA',
            'EntityType': 'Playlist',
            'PlaylistId': playlist_id,
            'Name': playlist.get('Name', ''),
            'CreatedAt': datetime.utcnow().isoformat(),
            'UpdatedAt': datetime.utcnow().isoformat()
        }
    
    def _transform_playlist_track(self, playlist_track: Dict[str, Any], track_info: Dict[str, Any]) -> Dict[str, Any]:
        """Transform playlist track with denormalized track information"""
        playlist_id = playlist_track['PlaylistId']
        track_id = playlist_track['TrackId']
        
        # Get additional track information from cache
        album = self._lookup_cache['albums'].get(track_info.get('AlbumId'), {}) if track_info.get('AlbumId') else {}
        artist = self._lookup_cache['artists'].get(album.get('ArtistId'), {}) if album else {}
        
        return {
            'PK': f"PLAYLIST#{playlist_id}",
            'SK': f"TRACK#{track_id}",
            'EntityType': 'PlaylistTrack',
            'PlaylistId': playlist_id,
            'TrackId': track_id,
            'TrackName': track_info.get('Name', 'Unknown Track'),
            'ArtistName': artist.get('Name', 'Unknown Artist'),
            'AlbumTitle': album.get('Title', 'Unknown Album'),
            'TrackDuration': track_info.get('Milliseconds', 0),
            'UnitPrice': float(track_info.get('UnitPrice', 0.0)),
            'CreatedAt': datetime.utcnow().isoformat(),
            'UpdatedAt': datetime.utcnow().isoformat()
        }
    
    def _transform_employee(self, employee: Dict[str, Any]) -> Dict[str, Any]:
        """Transform employee record to DynamoDB item"""
        employee_id = employee['EmployeeId']
        reports_to = employee.get('ReportsTo')
        
        item = {
            'PK': f"EMPLOYEE#{employee_id}",
            'SK': 'PROFILE',
            'EntityType': 'EmployeeProfile',
            'EmployeeId': employee_id,
            'FirstName': employee.get('FirstName', ''),
            'LastName': employee.get('LastName', ''),
            'Title': employee.get('Title', ''),
            'BirthDate': employee.get('BirthDate', ''),
            'HireDate': employee.get('HireDate', ''),
            'Address': employee.get('Address', ''),
            'City': employee.get('City', ''),
            'State': employee.get('State', ''),
            'Country': employee.get('Country', ''),
            'PostalCode': employee.get('PostalCode', ''),
            'Phone': employee.get('Phone', ''),
            'Fax': employee.get('Fax', ''),
            'Email': employee.get('Email', ''),
            'CreatedAt': datetime.utcnow().isoformat(),
            'UpdatedAt': datetime.utcnow().isoformat()
        }
        
        # Add manager relationship
        if reports_to:
            manager = self._lookup_cache.get('employees', {}).get(reports_to, {})
            item.update({
                'ReportsTo': reports_to,
                'ManagerName': f"{manager.get('FirstName', '')} {manager.get('LastName', '')}".strip(),
                'GSI1PK': f"MANAGER#{reports_to}",
                'GSI1SK': f"EMPLOYEE#{employee_id}"
            })
        
        return item
    
    def get_transformation_summary(self, source_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Get summary of data transformation
        
        Args:
            source_data: Source data dictionary
            
        Returns:
            Transformation summary
        """
        summary = {
            'source_tables': {},
            'target_tables': {
                'MusicCatalog': 0,
                'CustomerData': 0,
                'PlaylistData': 0,
                'EmployeeData': 0
            },
            'total_source_records': 0,
            'total_target_items': 0
        }
        
        # Count source records
        for table_name, records in source_data.items():
            count = len(records)
            summary['source_tables'][table_name] = count
            summary['total_source_records'] += count
        
        # Estimate target items
        # MusicCatalog: Artists + Albums + Tracks
        summary['target_tables']['MusicCatalog'] = (
            len(source_data.get('Artist', [])) +
            len(source_data.get('Album', [])) +
            len(source_data.get('Track', []))
        )
        
        # CustomerData: Customers + Invoices
        summary['target_tables']['CustomerData'] = (
            len(source_data.get('Customer', [])) +
            len(source_data.get('Invoice', []))
        )
        
        # PlaylistData: Playlists + PlaylistTracks
        summary['target_tables']['PlaylistData'] = (
            len(source_data.get('Playlist', [])) +
            len(source_data.get('PlaylistTrack', []))
        )
        
        # EmployeeData: Employees
        summary['target_tables']['EmployeeData'] = len(source_data.get('Employee', []))
        
        summary['total_target_items'] = sum(summary['target_tables'].values())
        
        return summary


