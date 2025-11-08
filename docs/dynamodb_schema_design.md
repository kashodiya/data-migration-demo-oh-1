# DynamoDB Schema Design for Chinook Database Migration

## Access Pattern Analysis

Based on the Chinook SQLite database structure, we've identified the following key access patterns for the music store application:

### Primary Access Patterns
1. **Browse music catalog** - Get artists, albums, and tracks
2. **Search functionality** - Find tracks by name, artist, album, or genre
3. **Customer management** - Customer profiles and purchase history
4. **Invoice processing** - Order details and line items
5. **Playlist management** - User playlists and track associations
6. **Employee management** - Staff hierarchy and customer assignments

## DynamoDB Table Design

### Table 1: MusicCatalog
**Purpose**: Store denormalized music catalog data for efficient browsing and searching

**Primary Key**:
- Partition Key: `PK` (String) - Format: `ARTIST#{ArtistId}` or `ALBUM#{AlbumId}` or `TRACK#{TrackId}`
- Sort Key: `SK` (String) - Format: `METADATA` or `ALBUM#{AlbumId}` or `TRACK#{TrackId}`

**Global Secondary Indexes**:
- GSI1: `GSI1PK` (Genre-based access) / `GSI1SK` (Track name for sorting)
- GSI2: `GSI2PK` (Search by name) / `GSI2SK` (Entity type + ID)

**Attributes**:
- EntityType (Artist/Album/Track)
- Name, Title, Composer
- ArtistName, AlbumTitle (denormalized)
- GenreName, MediaTypeName (denormalized)
- Milliseconds, Bytes, UnitPrice
- CreatedAt, UpdatedAt

### Table 2: CustomerData
**Purpose**: Customer information and purchase history

**Primary Key**:
- Partition Key: `PK` (String) - Format: `CUSTOMER#{CustomerId}`
- Sort Key: `SK` (String) - Format: `PROFILE` or `INVOICE#{InvoiceId}`

**Global Secondary Indexes**:
- GSI1: `GSI1PK` (Email for login) / `GSI1SK` (Customer ID)
- GSI2: `GSI2PK` (Support Rep ID) / `GSI2SK` (Customer ID)

**Attributes**:
- EntityType (Profile/Invoice)
- FirstName, LastName, Email, Company
- Address, City, State, Country, PostalCode
- Phone, Fax, SupportRepId
- InvoiceDate, BillingAddress, Total
- InvoiceLines (embedded for small invoices)

### Table 3: PlaylistData
**Purpose**: Playlist management and track associations

**Primary Key**:
- Partition Key: `PK` (String) - Format: `PLAYLIST#{PlaylistId}`
- Sort Key: `SK` (String) - Format: `METADATA` or `TRACK#{TrackId}`

**Attributes**:
- EntityType (Playlist/PlaylistTrack)
- PlaylistName
- TrackName, ArtistName, AlbumTitle (denormalized)
- TrackDuration, UnitPrice

### Table 4: EmployeeData
**Purpose**: Employee hierarchy and management

**Primary Key**:
- Partition Key: `PK` (String) - Format: `EMPLOYEE#{EmployeeId}`
- Sort Key: `SK` (String) - Format: `PROFILE` or `SUBORDINATE#{EmployeeId}`

**Global Secondary Indexes**:
- GSI1: `GSI1PK` (Manager ID) / `GSI1SK` (Employee ID)

**Attributes**:
- EntityType (Profile/Subordinate)
- FirstName, LastName, Title, Email
- ReportsTo, BirthDate, HireDate
- Address, City, State, Country
- Phone, Fax

## Data Transformation Strategy

### Denormalization Approach
1. **Embed related data** to reduce the need for joins
2. **Duplicate frequently accessed attributes** across entities
3. **Create composite keys** for hierarchical relationships
4. **Use GSIs** for alternative access patterns

### Key Design Patterns
1. **Hierarchical Keys**: Use `#` separator for entity relationships
2. **Overloaded GSIs**: Single GSI supports multiple access patterns
3. **Sparse Indexes**: Only items with specific attributes appear in GSIs
4. **Composite Attributes**: Combine multiple fields for efficient querying

### Migration Considerations
1. **Batch Size**: Process 25 items per batch (DynamoDB limit)
2. **Rate Limiting**: Implement exponential backoff for throttling
3. **Data Validation**: Verify all foreign key relationships during transformation
4. **Incremental Processing**: Track progress at entity level for resume capability

## Estimated Item Counts
- MusicCatalog: ~4,125 items (275 artists + 347 albums + 3,503 tracks)
- CustomerData: ~471 items (59 customers + 412 invoices)
- PlaylistData: ~8,733 items (18 playlists + 8,715 playlist tracks)
- EmployeeData: ~8 items (8 employees)

**Total**: ~13,337 items across all tables
