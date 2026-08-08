class SelectionManager {
  private selectedIds: Set<number> = new Set();

  public getSelected(): ReadonlySet<number> {
    return new Set(this.selectedIds);
  }

  public toggle(id: number): void {
    if (this.selectedIds.has(id)) {
      this.selectedIds.delete(id);
    } else {
      this.selectedIds.add(id);
    }
  }

  public set(ids: number[]): void {
    this.selectedIds = new Set(ids);
  }

  public clear(): void {
    this.selectedIds.clear();
  }
}

export class RangeSelectionManager {
  private selection = new SelectionManager();
  private anchorId: number | null = null;
  private leadId: number | null = null; 
  private listeners = new Set<() => void>();
  private cachedSelected: number[] = [];

  subscribe(callback: () => void): () => void {
    this.listeners.add(callback);
    return () => this.listeners.delete(callback);
  }

  private notify() {
    this.listeners.forEach((listener) => listener());
  }

  private updateCache() {
    this.cachedSelected = Array.from(this.selection.getSelected());
  }

  public toggleSelect(id: number): void {
    this.selection.toggle(id);
    this.anchorId = id;
    this.leadId = id;
    this.updateCache();
    this.notify();
  }

  public selectSingle(id: number): void {
    this.selection.set([id]);
    this.anchorId = id;
    this.leadId = id;
    this.updateCache();
    this.notify();
  }

  public selectRange(targetId: number, allIds: number[]): void {
    if (this.anchorId === null || !allIds.includes(this.anchorId)) {
      this.selectSingle(targetId);
      return;
    }

    const start = allIds.indexOf(this.anchorId);
    const end = allIds.indexOf(targetId);
    if (start === -1 || end === -1) return;

    const [from, to] = start < end ? [start, end] : [end, start];
    this.selection.set(allIds.slice(from, to + 1));
    this.leadId = targetId;
    this.updateCache();
    this.notify();
  }

  public moveSelection(direction: 1 | -1, allIds: number[]): void {
    if (allIds.length === 0) return;
  
    const activeId = this.leadId ?? this.anchorId;
  
    if (activeId === null || !allIds.includes(activeId)) {
      this.selectSingle(allIds[0]);
      return;
    }
  
    const currentIndex = allIds.indexOf(activeId);
    const nextIndex = (currentIndex + direction + allIds.length) % allIds.length;
    
    const isMultiSelection = this.selection.getSelected().size > 1;
  
    if (nextIndex !== currentIndex || isMultiSelection) {
      this.selectSingle(allIds[nextIndex]);
    }
  }
  
  public extendSelection(direction: 1 | -1, allIds: number[]): void {
    if (allIds.length === 0) {
      return;
    }
  
    if (this.anchorId === null || !allIds.includes(this.anchorId)) {
      this.selectSingle(allIds[0]);
      return;
    }
  
    const leadId = this.leadId ?? this.anchorId;
    const leadIndex = allIds.indexOf(leadId);
    if (leadIndex === -1) {
      this.selectSingle(allIds[0]);
      return;
    }
  
    const nextIndex = leadIndex + direction;
    if (nextIndex < 0 || nextIndex >= allIds.length) {
      return;
    }
  
    const nextLeadId = allIds[nextIndex];
    const anchorIndex = allIds.indexOf(this.anchorId);
  
    const [from, to] = anchorIndex < nextIndex
      ? [anchorIndex, nextIndex]
      : [nextIndex, anchorIndex];
  
    this.selection.set(allIds.slice(from, to + 1));
    this.leadId = nextLeadId;
    this.updateCache();
    this.notify();
  }

  public getSelected(): number[] {
    return this.cachedSelected;
  }
  
  public getFirstSelected(): number | null {
    return this.anchorId;
  }

  public clear() {
    this.anchorId = null;
    this.leadId = null;
    this.selection.clear();
    this.updateCache();
    this.notify();
  }

  public removeDeleted(ids: number[]): void {
    let changed = false;
    for (const id of ids) {
      if (this.selection.getSelected().has(id)) {
        this.selection.toggle(id);
        changed = true;
      }
    }
    if (this.anchorId !== null && ids.includes(this.anchorId)) {
      this.anchorId = null;
      changed = true;
    }
    if (this.leadId !== null && ids.includes(this.leadId)) {
      this.leadId = null;
      changed = true;
    }
    if (changed) {
      this.updateCache();
      this.notify();
    }
  }
}
