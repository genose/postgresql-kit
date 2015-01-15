
#import <PGControlsKit/PGControlsKit.h>

@interface PGSourceViewController ()
@property (readonly) NSMutableArray* headings;
@end

@implementation PGSourceViewController

////////////////////////////////////////////////////////////////////////////////
// constructors

-(id)init {
    self = [super initWithNibName:@"PGSourceView" bundle:[NSBundle bundleForClass:[self class]]];
	if(self) {
		_headings = [NSMutableArray array];
		NSParameterAssert(_headings);
	}
	return self;
}

////////////////////////////////////////////////////////////////////////////////
// properties

@synthesize headings = _headings;

////////////////////////////////////////////////////////////////////////////////
// methods

-(void)addHeadingWithTitle:(NSString* )title {
	PGSourceViewNode* node = [[PGSourceViewNode alloc] initWithName:title];
	[[self headings] addObject:node];
	[(NSOutlineView* )[self view] reloadData];
}

////////////////////////////////////////////////////////////////////////////////
// NSOutlineViewDataSource

-(id)outlineView:(NSOutlineView* )outlineView child:(NSInteger)index ofItem:(id)item {
	if(item==nil) {
		NSLog(@"returning %@",[[self headings] objectAtIndex:index]);
		return [[self headings] objectAtIndex:index];
	}
	return nil;
}

-(NSInteger)outlineView:(NSOutlineView *)outlineView numberOfChildrenOfItem:(id)item {
	if(item==nil) {
		return [[self headings] count];
	}
	return 0;
}

-(BOOL)outlineView:(NSOutlineView* )outlineView isItemExpandable:(id)item {
	NSInteger count = [self outlineView:outlineView numberOfChildrenOfItem:item];

	NSLog(@"is item expandable %@ => %ld",item,count);
	
	return count ? YES : NO;
}


////////////////////////////////////////////////////////////////////////////////
// NSOutlineView delegate

-(BOOL)outlineView:(NSOutlineView* )outlineView isGroupItem:(id)item {
	NSParameterAssert([item isKindOfClass:[PGSourceViewNode class]]);
	return [item isGroupItem];
}

-(BOOL)outlineView:(NSOutlineView* )outlineView shouldSelectItem:(id)item {
	NSParameterAssert([item isKindOfClass:[PGSourceViewNode class]]);
	return [item shouldSelectItem];
}

-(NSString* )outlineView:(NSOutlineView* )outlineView toolTipForCell:(NSCell *)cell rect:(NSRectPointer)rect tableColumn:(NSTableColumn *)tc item:(id)item mouseLocation:(NSPoint)mouseLocation {
	NSParameterAssert([item isKindOfClass:[PGSourceViewNode class]]);
	return nil;
}

-(BOOL)outlineView:(NSOutlineView* )outlineView shouldEditTableColumn:(NSTableColumn* )tableColumn item:(id)item {
	return NO;
}

-(NSView* )outlineView:(NSOutlineView *)outlineView viewForTableColumn:(NSTableColumn* )tableColumn item:(id)item {
	NSParameterAssert([item isKindOfClass:[PGSourceViewNode class]]);
	NSTableCellView* result = [outlineView makeViewWithIdentifier:@"HeaderCell" owner:self];
	[[result textField] setStringValue:[item name]];
    return result;
}

@end
