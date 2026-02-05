import { CdkDragDrop, moveItemInArray, transferArrayItem } from '@angular/cdk/drag-drop';
import { Component, EventEmitter, Input, OnChanges, Output, SimpleChanges } from '@angular/core';
import { View, ViewFolder, ViewType } from '../../_models/hmi';
import { TranslateService } from '@ngx-translate/core';
import { ConfirmDialogComponent, ConfirmDialogData } from '../../gui-helpers/confirm-dialog/confirm-dialog.component';
import { MatDialog as MatDialog } from '@angular/material/dialog';
import { ProjectService } from '../../_services/project.service';
import { ViewPropertyComponent, ViewPropertyType } from '../view-property/view-property.component';
import * as FileSaver from 'file-saver';
import { EditNameComponent, EditNameData } from '../../gui-helpers/edit-name/edit-name.component';
import { Utils } from '../../_helpers/utils';

@Component({
    selector: 'app-editor-views-list',
    templateUrl: './editor-views-list.component.html',
    styleUrls: ['./editor-views-list.component.scss']
})
export class EditorViewsListComponent implements OnChanges {

    @Input() views: View[] = [];
    @Input() viewFolders: ViewFolder[] = [];
    @Input('select') set select(view: View) {
        this.currentView = view;
    };
    @Output() selected: EventEmitter<View> = new EventEmitter<View>();
    @Output() viewPropertyChanged: EventEmitter<View> = new EventEmitter<View>();
    @Output() cloneView: EventEmitter<View> = new EventEmitter<View>();

    currentView: View = null;

    cardViewType = ViewType.cards;
    svgViewType = ViewType.svg;
    mapsViewType = ViewType.maps;

    rootViews: View[] = [];
    folderViews: { [folderId: string]: View[] } = {};

    constructor(private projectService: ProjectService,
        private translateService: TranslateService,
        public dialog: MatDialog,
    ) { }

    ngOnChanges(changes: SimpleChanges): void {
        if (changes.views || changes.viewFolders) {
            this.rebuildGroups();
        }
    }

    onSelectView(view: View, force = true) {
        if (!force && this.currentView?.id === view?.id) {
            return;
        }
        this.currentView = view;
        this.selected.emit(this.currentView);
    }

    getViewsSorted(views: View[]) {
        return [...(views || [])].sort((a, b) => {
            if (a.name > b.name) { return 1; }
            return -1;
        });
    }

    getFoldersSorted() {
        return [...(this.viewFolders || [])].sort((a, b) => a.name.localeCompare(b.name));
    }

    isViewActive(view) {
        return (this.currentView && this.currentView.name === view.name);
    }

    onAddFolder() {
        const exist = this.viewFolders.map(folder => folder.name);
        const dialogRef = this.dialog.open(EditNameComponent, {
            disableClose: true,
            position: { top: '60px' },
            data: <EditNameData> {
                title: this.translateService.instant('editor.view-folder-add'),
                name: this.translateService.instant('editor.view-folder-default-name'),
                exist
            }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result?.name) {
                this.viewFolders.push({ id: Utils.getShortGUID('vf_'), name: result.name });
                this.saveFolders();
                this.rebuildGroups();
            }
        });
    }

    onRenameFolder(folder: ViewFolder) {
        const exist = this.viewFolders.filter((f) => f.id !== folder.id).map((f) => f.name);
        const dialogRef = this.dialog.open(EditNameComponent, {
            disableClose: true,
            position: { top: '60px' },
            data: <EditNameData> {
                title: this.translateService.instant('editor.view-folder-rename'),
                name: folder.name,
                exist
            }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result?.name) {
                folder.name = result.name;
                this.saveFolders();
            }
        });
    }

    onDeleteFolder(folder: ViewFolder) {
        const viewsInFolder = (this.views || []).filter(view => view.folderId === folder.id);
        const msg = viewsInFolder.length > 0 ?
            this.translateService.instant('msg.view-folder-remove-with-views', { value: folder.name }) :
            this.translateService.instant('msg.view-folder-remove', { value: folder.name });

        const dialogRef = this.dialog.open(ConfirmDialogComponent, {
            position: { top: '60px' },
            data: <ConfirmDialogData>{ msg }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (!result) {
                return;
            }
            this.viewFolders = this.viewFolders.filter(item => item.id !== folder.id);
            viewsInFolder.forEach(view => {
                view.folderId = null;
                this.projectService.setView(view, false);
            });
            this.saveFolders();
            this.rebuildGroups();
        });
    }

    onDropRoot(event: CdkDragDrop<View[]>) {
        if (event.previousContainer === event.container) {
            moveItemInArray(this.rootViews, event.previousIndex, event.currentIndex);
            return;
        }

        const previousViews = event.previousContainer.data;
        const droppedView = previousViews[event.previousIndex];
        if (!droppedView) {
            return;
        }
        droppedView.folderId = null;
        transferArrayItem(previousViews, this.rootViews, event.previousIndex, event.currentIndex);
        this.projectService.setView(droppedView, false);
    }

    onDropFolder(folder: ViewFolder, event: CdkDragDrop<View[]>) {
        const target = this.folderViews[folder.id] || [];
        if (event.previousContainer === event.container) {
            moveItemInArray(target, event.previousIndex, event.currentIndex);
            return;
        }

        const previousViews = event.previousContainer.data;
        const droppedView = previousViews[event.previousIndex];
        if (!droppedView) {
            return;
        }
        droppedView.folderId = folder.id;
        transferArrayItem(previousViews, target, event.previousIndex, event.currentIndex);
        this.projectService.setView(droppedView, false);
    }

    getDropListIds() {
        const ids = ['views-root-drop-list'];
        this.getFoldersSorted().forEach(folder => ids.push(this.getFolderDropListId(folder.id)));
        return ids;
    }

    getFolderDropListId(folderId: string) {
        return `views-folder-drop-list-${folderId}`;
    }

    private rebuildGroups() {
        this.rootViews = this.getViewsSorted((this.views || []).filter(view => !view.folderId));
        this.folderViews = {};
        this.getFoldersSorted().forEach(folder => {
            this.folderViews[folder.id] = this.getViewsSorted((this.views || []).filter(view => view.folderId === folder.id));
        });
    }

    private saveFolders() {
        this.projectService.setViewFolders(this.viewFolders || []);
    }

    onDeleteView(view) {
        let msg = '';
        this.translateService.get('msg.view-remove', { value: view.name }).subscribe((txt: string) => { msg = txt; });
        let dialogRef = this.dialog.open(ConfirmDialogComponent, {
            position: { top: '60px' },
            data: <ConfirmDialogData> { msg: this.translateService.instant('msg.view-remove', { value: view.name }) }
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result && this.views) {
                let toselect = null;
                for (var i = 0; i < this.views.length; i++) {
                    if (this.views[i].id === view.id) {
                        this.views.splice(i, 1);
                        if (i > 0 && i < this.views.length) {
                            toselect = this.views[i];
                        }
                        break;
                    }
                }
                this.currentView = null;
                if (toselect) {
                    this.onSelectView(toselect);
                } else if (this.views.length > 0) {
                    this.onSelectView(this.views[0]);
                }
                this.projectService.removeView(view);
                this.rebuildGroups();
            }
        });
    }

    onRenameView(view) {
        let exist = this.views.filter((v) => v.id !== view.id).map((v) => v.name);
        let dialogRef = this.dialog.open(EditNameComponent, {
            disableClose: true,
            position: { top: '60px' },
            data: <EditNameData> {
                title: this.translateService.instant('dlg.docname-title'),
                name: view.name,
                exist: exist
            }
        });
        dialogRef.afterClosed().subscribe(result => {
            if (result && result.name) {
                view.name = result.name;
                this.projectService.setView(view, false);
                this.rebuildGroups();
            }
        });
    }

    onPropertyView(view) {
        let dialogRef = this.dialog.open(ViewPropertyComponent, {
            position: { top: '60px' },
            disableClose: true,
            data: <ViewPropertyType> {
                name: view.name,
                type: view.type || ViewType.svg,
                profile: view.profile,
                property: view.property}
        });

        dialogRef.afterClosed().subscribe(result => {
            if (result?.profile) {
                if (result.profile.height) {view.profile.height = parseInt(result.profile.height);}
                if (result.profile.width) {view.profile.width = parseInt(result.profile.width);}
                if (result.profile.margin >= 0) {view.profile.margin = parseInt(result.profile.margin);}
                view.profile.bkcolor = result.profile.bkcolor;
                if (result.property?.events) {
                    view.property ??= { events: [], actions: [] };
                    view.property.events = result.property.events;
                }
                this.viewPropertyChanged.emit(view);
                this.onSelectView(view);
            }
        });
    }

    onCloneView(view: View) {
        this.cloneView.emit(view);
    }

    onExportView(view: View) {
        let filename = `${view.name}.json`;
        let content = JSON.stringify(view);
        let blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
        FileSaver.saveAs(blob, filename);
    }

    onCleanView(view: View) {
        const changed = this.projectService.cleanView(view);
        if (changed) {
            this.onSelectView(view);
        }
    }
}
